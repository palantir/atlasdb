/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.dbkvs.timestamp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.common.base.Throwables;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.pool.ConnectionManager;
import com.palantir.nexus.db.pool.RetriableTransactions;
import com.palantir.nexus.db.pool.RetriableTransactions.TransactionResult;
import com.palantir.nexus.db.pool.RetriableWriteTransaction;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;

// TODO: switch to using ptdatabase sql running, which more gracefully
// supports multiple db types.
public final class InDbTimestampBoundStore implements TimestampBoundStore {
    private static final Logger log = LoggerFactory.getLogger(InDbTimestampBoundStore.class);

    private final ConnectionManager connManager;

    @GuardedBy("this") // lazy init to avoid db connections in constructors
    private DBType dbType;

    @GuardedBy("this")
    private Long currentLimit = null;

    public InDbTimestampBoundStore(ConnectionManager connManager) {
        this.connManager = Preconditions.checkNotNull(connManager);
    }

    private interface Operation {
        long run(Connection c, @Nullable Long oldLimit) throws SQLException;
    }

    private long runOperation(final Operation o) {
        TransactionResult<Long> result = RetriableTransactions.run(connManager, new RetriableWriteTransaction<Long>() {
            @Override
            public Long run(Connection c) throws SQLException {
                Long oldLimit = readLimit(c);
                if (currentLimit != null) {
                    if (oldLimit != null) {
                        if (currentLimit.equals(oldLimit)) {
                            // match, good
                        } else {
                            // mismatch
                            throw new MultipleRunningTimestampServiceError(
                                    "Timestamp limit changed underneath us (limit in memory: " + currentLimit +
                                    ", limit in DB: " + oldLimit + "). This may indicate that " +
                                    "another timestamp service is running against this database!  " +
                                    "If you get this error check that your client.prefs in dispatch " +
                                    "is configured correctly."
                                    );
                        }
                    } else {
                        // disappearance
                        throw new IllegalStateException(
                                "Unable to retrieve a timestamp when expected. " +
                                        "You probably reseeded the database while the timestamp server was running. " +
                                        "This deloyment is in a dangerous state and you should take down " +
                                        "everything (all servers) and reseed the database again."
                                );
                    }
                } else {
                    // first read, no check to be done
                }
                return o.run(c, oldLimit);
            }
        });
        switch (result.getStatus()) {
        case SUCCESSFUL:
            currentLimit = result.getResultValue();
            return currentLimit;
        case UNKNOWN:
            // Since the DB's state is unknown, the in-memory state can't be confirmed, so get rid of it.
            currentLimit = null;
            // Fall through to failure case, where exceptions are handled.
        case FAILED:
            Throwable error = result.getError();
            if (error instanceof SQLException) {
                throw PalantirSqlException.create((SQLException) error);
            }
            throw Throwables.rewrapAndThrowUncheckedException(error);
        default:
            throw new IllegalStateException("Unrecognized transaction status " + result.getStatus());
        }
    }

    @Override
    public synchronized long getUpperLimit() {
        return runOperation(new Operation() {
            @Override
            public long run(Connection c, Long oldLimit) throws SQLException {
                if (oldLimit != null) {
                    return oldLimit;
                }

                final long startVal = 10000;
                createLimit(c, startVal);
                return startVal;
            }
        });
    }

    @Override
    public synchronized void storeUpperLimit(final long limit) {
        runOperation(new Operation() {
            @Override
            public long run(Connection c, Long oldLimit) throws SQLException {
                if (oldLimit != null) {
                    writeLimit(c, limit);
                } else {
                    createLimit(c, limit);
                }

                return limit;
            }
        });
    }

    private Long readLimit(Connection c) throws SQLException {
        String sql = "SELECT last_allocated FROM " + AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName() + " FOR UPDATE";
        QueryRunner run = new QueryRunner();
        return run.query(c, sql, new ResultSetHandler<Long>() {
            @Override
            public Long handle(ResultSet rs) throws SQLException {
                if (rs.next()) {
                    return rs.getLong("last_allocated");
                } else {
                    return null;
                }
            }
        });
    }

    private void writeLimit(Connection c, long limit) throws SQLException {
        QueryRunner run = new QueryRunner();
        String updateTs = "UPDATE " + AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName() + " SET last_allocated = ?";
        PreparedStatement statement = c.prepareStatement(updateTs);
        statement.setLong(1, limit);
        statement.executeUpdate();
        statement.close();
    }

    private void createLimit(Connection c, long limit) throws SQLException {
        QueryRunner run = new QueryRunner();
        run.update(c, "INSERT INTO " + AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName() + " (last_allocated) VALUES (?)", limit);
    }

    private DBType getDbType(Connection c) {
        if (dbType == null) {
            dbType = ConnectionDbTypes.getDBType(c);
        }
        return dbType;
    }
}
