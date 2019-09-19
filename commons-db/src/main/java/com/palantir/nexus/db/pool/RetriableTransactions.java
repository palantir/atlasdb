/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.nexus.db.pool;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.palantir.logsafe.Preconditions;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RetriableTransactions {
    private static final Logger log = LoggerFactory.getLogger(RetriableTransactions.class);

    private RetriableTransactions() {
        // hidden
    }

    private static final long MAX_DELAY_MS = 3 * 60 * 1000;
    private static final String TABLE_NAME = "pt_retriable_txn_log_v1";

    private static <T> TransactionResult<T> runSimple(ConnectionManager cm, RetriableWriteTransaction<T> tx) {
        Connection c = null;
        try {
            c = cm.getConnection();
            c.setAutoCommit(false);
            T ret = tx.run(c);
            c.commit();
            return TransactionResult.success(ret);
        } catch (Throwable t) {
            return TransactionResult.failure(t);
        } finally {
            if (c != null) {
                try {
                    c.close();
                } catch (Throwable t) {
                    log.warn("A problem happened trying to close a connection.", t);
                }
            }
        }
    }

    public static enum TransactionStatus {
        SUCCESSFUL, FAILED, UNKNOWN;
    }
    public static class TransactionResult<T> {
        private final TransactionStatus status;
        private final @Nullable T resultValue;
        private final Optional<Throwable> error;
        private TransactionResult(TransactionStatus status,
                                  @Nullable T resultValue,
                                  Optional<Throwable> error) {
            this.status = status;
            this.resultValue = resultValue;
            this.error = error;
        }

        public static <T> TransactionResult<T> success(T resultValue) {
            return new TransactionResult<T>(TransactionStatus.SUCCESSFUL, resultValue, Optional.<Throwable>empty());
        }

        public static <T> TransactionResult<T> failure(Throwable error) {
            return new TransactionResult<T>(TransactionStatus.FAILED, null /* result value */, Optional.of(error));
        }

        public static <T> TransactionResult<T> unknown(Throwable error) {
            return new TransactionResult<T>(TransactionStatus.UNKNOWN, null /* result value */, Optional.of(error));
        }

        public TransactionStatus getStatus() {
            return status;
        }

        // May only be called if the result is SUCCESSFUL.
        public @Nullable T getResultValue() {
            Preconditions.checkState(status.equals(TransactionStatus.SUCCESSFUL), "Trying to get result from a transaction which never succeeded");
            return resultValue;
        }

        // May only be called if the result is not SUCCESSFUL.
        public Throwable getError() {
            return error.get();
        }
    }
    /**
     * Run a {@link RetriableWriteTransaction}, see {@link RetriableWriteTransaction} for details.
     */
    public static <T> TransactionResult<T> run(final ConnectionManager cm, final RetriableWriteTransaction<T> tx) {
        switch (cm.getDbType()) {
            case ORACLE:
            case POSTGRESQL:
            case H2_MEMORY:
                // fallthrough, these are handled below with a complex
                // retry loop...
                break;

            default:
                // If we don't know what dbtype this is, don't enter the
                // complex retry loop. no need to complicate the code below
                // trying to understand peculiarities of yet another
                // not-quite-the-same DB.
                return runSimple(cm, tx);
        }

        class LexicalHelper {
            long startTimeMs = System.currentTimeMillis();
            boolean pending = false;
            UUID id = null;
            T result = null;

            public TransactionResult<T> run() {
                boolean createdTxTable = false;
                while (true) {
                    long attemptTimeMs = System.currentTimeMillis();
                    try {
                        if (!createdTxTable) {
                            // this is actually only an attempt, could bounce with SQLException(!)
                            createTxTable(cm);
                            createdTxTable = true;
                        }

                        if (!pending) {
                            attemptTx();

                            // great
                            return TransactionResult.success(result);
                        } else {
                            if (attemptVerify()) {
                                // transaction actually went in
                                return TransactionResult.success(result);
                            }

                            // transaction did not go in, back to square one
                            pending = false;
                            id = null;
                            result = null;
                        }
                    } catch (SQLException e) {
                        long now = System.currentTimeMillis();
                        if (log.isTraceEnabled()) {
                            log.trace("Got exception for retriable write transaction, startTimeMs = {}, attemptTimeMs = {}, now = {}", startTimeMs, attemptTimeMs, now, e);
                        }
                        if (shouldStillRetry(startTimeMs, attemptTimeMs)) {
                            long attemptLengthMs = now - attemptTimeMs;
                            long totalLengthMs = now - startTimeMs;
                            log.info("Swallowing possible transient exception for retriable transaction, last attempt took {} ms, total attempts have taken {}", attemptLengthMs, totalLengthMs, e);
                            continue;
                        }
                        if (pending) {
                            log.error("Giving up on [verification of] retriable write transaction that might have actually commited!", e);
                            return TransactionResult.unknown(e);
                        }
                        return TransactionResult.failure(e);
                    } catch (Throwable t) {
                        return TransactionResult.failure(t);
                    }
                }
            }

            private void attemptTx() throws SQLException {
                boolean ret = false;
                try {
                    Connection c = cm.getConnection();
                    try {
                        // Crash anywhere in here means no commit attempted and we stay in non-pending
                        c.setAutoCommit(false);
                        T newResult = tx.run(c);
                        UUID newId = UUID.randomUUID();
                        addTxLog(c, newId);

                        // now we flip, since if the commit bails we need to figure out what happened
                        pending = true;
                        id = newId;
                        result = newResult;
                        c.commit();

                        // if we got here we'll return even if cleanTxLog() throws
                        ret = true;

                        cleanTxLog(c, id);
                        c.commit();

                        return;
                    } finally {
                        c.close();
                    }
                } catch (SQLException e) {
                    if (ret) {
                        squelch(e);
                        return;
                    }
                    throw e;
                }
            }

            private boolean attemptVerify() throws SQLException {
                boolean ret = false;
                try {
                    Connection c = cm.getConnection();
                    try {
                        ret = checkTxLog(c, id);
                        if (ret) {
                            cleanTxLog(c, id);
                        }
                        return ret;
                    } finally {
                        c.close();
                    }
                } catch (SQLException e) {
                    if (ret) {
                        // Once we know we're in we may or may not have tried to delete
                        // leaving us the sole carrier of that tx having completed so
                        // we can't throw.
                        squelch(e);
                        return ret;
                    }
                    throw e;
                }
            }

            private void squelch(SQLException e) {
                log.warn("Squelching SQLException while trying to clean up retriable write transaction id {}", id, e);
            }
        }
        return new LexicalHelper().run();
    }

    private static final LoadingCache<ConnectionManager, AtomicBoolean> createdTxTables = CacheBuilder.newBuilder().weakKeys().build(new CacheLoader<ConnectionManager, AtomicBoolean>() {
        @Override
        public AtomicBoolean load(ConnectionManager cm) {
            return new AtomicBoolean(false);
        }
    });
    private static void createTxTable(ConnectionManager cm) throws SQLException {
        AtomicBoolean createdTxTable = createdTxTables.getUnchecked(cm);
        if (createdTxTable.get()) {
            return;
        }

        String varcharType = null;
        switch (cm.getDbType()) {
            case ORACLE:
                varcharType = "VARCHAR2";
                break;

            case POSTGRESQL:
            case H2_MEMORY:
                varcharType = "VARCHAR";
                break;

            default:
                throw new IllegalStateException();
        }
        if (varcharType == null) {
            throw new IllegalStateException();
        }

        // I'd love to CREATE TABLE ... IF NOT EXISTS but Ye Olde Postgres (TM)
        // does not like it so we do something ugly.
        try {
            Connection c = cm.getConnection();
            try {
                // Attempt to add to the table, but don't actually commit it.
                c.setAutoCommit(false);
                addTxLog(c, UUID.randomUUID());
                c.rollback();
            } finally {
                c.close();
            }
            // Great, that worked, fallthrough to below but don't commit.
        } catch (SQLException e) {
            log.info("The table {} has not been created yet, so we will try to create it.", TABLE_NAME);
            log.debug("To check whether the table exists we tried to use it. This caused an exception indicating that it did not exist. The exception was: ", e);
            Connection c = cm.getConnection();
            try {
                c.createStatement().execute("CREATE TABLE " + TABLE_NAME + " (id " + varcharType + "(36) PRIMARY KEY, created TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");
            } finally {
                c.close();
            }
            // Good enough, fallthrough to below.
        }

        // Something worked, no need to do this again
        createdTxTable.set(true);
    }

    private static void addTxLog(Connection c, UUID id) throws SQLException {
        PreparedStatement ps = c.prepareStatement("INSERT INTO " + TABLE_NAME + " (id) VALUES (?)");
        try {
            ps.setString(1, id.toString());
            ps.executeUpdate();
        } finally {
            ps.close();
        }
    }

    private static boolean checkTxLog(Connection c, UUID id) throws SQLException {
        PreparedStatement ps = c.prepareStatement("SELECT 1 FROM " + TABLE_NAME + " WHERE id = ?");
        try {
            ps.setString(1, id.toString());
            ResultSet rs = ps.executeQuery();
            try {
                return rs.next();
            } finally {
                rs.close();
            }
        } finally {
            ps.close();
        }
    }

    private static void cleanTxLog(Connection c, UUID id) throws SQLException {
        PreparedStatement ps = c.prepareStatement("DELETE FROM " + TABLE_NAME + " WHERE id = ?");
        try {
            ps.setString(1, id.toString());
            ps.executeUpdate();
        } finally {
            ps.close();
        }
    }

    private static boolean shouldStillRetry(long startTimeMs, long attemptTimeMs) {
        return attemptTimeMs <= startTimeMs + MAX_DELAY_MS;
    }
}
