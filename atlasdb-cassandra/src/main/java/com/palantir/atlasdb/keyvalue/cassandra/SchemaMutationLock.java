/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.cassandra;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;

public class SchemaMutationLock {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaMutationLock.class);

    private final boolean supportsCas;
    private final CassandraKeyValueServiceConfigManager configManager;
    private final CassandraClientPool clientPool;
    private final ConsistencyLevel writeConsistency;
    private final UniqueSchemaMutationLockTable lockTable;
    private final ReentrantLock schemaMutationLockForEarlierVersionsOfCassandra = new ReentrantLock(true);
    private HeartbeatThread heartbeatThread;

    public SchemaMutationLock(
            boolean supportsCas,
            CassandraKeyValueServiceConfigManager configManager,
            CassandraClientPool clientPool,
            ConsistencyLevel writeConsistency,
            UniqueSchemaMutationLockTable lockTable) {
        this.supportsCas = supportsCas;
        this.configManager = configManager;
        this.clientPool = clientPool;
        this.writeConsistency = writeConsistency;
        this.lockTable = lockTable;
    }

    public interface Action {
        void execute() throws Exception;
    }

    class HeartbeatThread {
        private final Thread heartbeatThread;
        private final long lockId;
        private volatile Boolean keepBeating;
        private volatile long heartbeatCount;

        HeartbeatThread(long lockId) {
            this.heartbeatThread = new Thread(this::heartbeat);
            this.lockId = lockId;
            this.heartbeatCount = 0;
        }

        void startBeating() {
            keepBeating = true;
            heartbeatThread.start();
        }

        void stopBeating() {
            keepBeating = false;
            try {
                // make sure beating's stopped
                heartbeatThread.join();
            } catch (InterruptedException e) {
                LOGGER.debug("Interrupted while trying to stop the heartbeat heartbeatThread.");
            }
        }

        void heartbeat() {
            while (keepBeating) {
                try {
                    clientPool.runWithRetry((FunctionCheckedException<Cassandra.Client, Void, Exception>) client -> {
                        Column ourUpdate = lockColumnWithStrValue(String.format(
                                CassandraConstants.GLOBAL_DDL_LOCK_FORMAT, lockId, heartbeatCount + 1));

                        List<Column> expected = ImmutableList.of(lockColumnWithStrValue(String.format(
                                CassandraConstants.GLOBAL_DDL_LOCK_FORMAT, lockId, heartbeatCount)));

                        CASResult casResult = writeDdlLockWithCas(client, expected, ourUpdate);

                        if (casResult.isSuccess()) {
                            heartbeatCount++;
                        } else {
                            handleForcedLockClear(casResult, lockId, heartbeatCount);
                        }
                        return null;
                    });
                    Thread.sleep(configManager.getConfig().heartbeatTimePeriodMillis());
                } catch (Exception e) {
                    throw Throwables.throwUncheckedException(e);
                }
            }
        }
    }

    void runWithLock(Action action) {
        if (!supportsCas) {
            runWithLockWithoutCas(action);
            return;
        }

        long lockId = waitForSchemaMutationLock();
        heartbeatThread = new HeartbeatThread(lockId);
        heartbeatThread.startBeating();
        try {
            action.execute();
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        } finally {
            heartbeatThread.stopBeating();
            schemaMutationUnlock(lockId);
        }
    }

    private void runWithLockWithoutCas(Action action) {
        LOGGER.info("Because your version of Cassandra does not support check and set,"
                + " we will use a java level lock to synchronise schema mutations."
                + " If this is a clustered service, this could lead to corruption.");
        try {
            waitForSchemaMutationLockWithoutCas();
            action.execute();
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        } finally {
            schemaMutationUnlockWithoutCas();
        }

    }

    void killHeartbeat() {
        heartbeatThread.stopBeating();
    }

    /**
     * Each locker generates a random ID per operation and uses a CAS operation as a DB-side lock.
     *
     * There are two special ID values used for book-keeping
     * - one representing the lock being cleared
     * - one representing a remote ID that is guaranteed to never be generated
     * This is required because Cassandra CAS does not treat setting to null / empty as a delete,
     * (though there is a to-do in their code to possibly add this)
     * though it accepts an empty expected column to mean that non-existance was expected
     * (see putUnlessExists for example, which has the luck of not having to ever deal with deleted values)
     *
     * I can't hold this against them though, because Atlas has a semi-similar problem with empty byte[] values
     * meaning deleted internally.
     *
     * @return an ID to be passed into a subsequent unlock call
     */
    private long waitForSchemaMutationLock() {
        final long perOperationNodeId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE - 2);

        try {
            clientPool.runWithRetry((FunctionCheckedException<Cassandra.Client, Void, Exception>) client -> {
                Column ourUpdate = lockColumnWithStrValue(
                        String.format(CassandraConstants.GLOBAL_DDL_LOCK_FORMAT, perOperationNodeId, 0));

                List<Column> expected = ImmutableList.of(lockColumnWithStrValue(
                        CassandraConstants.GLOBAL_DDL_LOCK_CLEARED_VALUE));

                CASResult casResult = writeDdlLockWithCas(client, expected, ourUpdate);

                Column lastSeenValue = null;

                // could have a timeout controlling this level, confusing for users to set both timeouts though
                while (!casResult.isSuccess()) {
                    if (casResult.getCurrent_valuesSize() == 0) { // never has been an existing lock
                        // special case, no one has ever made a lock ever before
                        // this becomes analogous to putUnlessExists now
                        expected = ImmutableList.<Column>of();
                    } else {
                        Column existingValue = Iterables.getOnlyElement(casResult.getCurrent_values(), null);
                        if (existingValue == null) {
                            throw new IllegalStateException("Something is wrong with underlying locks."
                                    + " Consult support for guidance on manually examining and clearing"
                                    + " locks from " + lockTable.getOnlyTable() + " table.");
                        }
                        if (lastSeenValue == null || !existingValue.equals(lastSeenValue)) {
                            LOGGER.debug("Heartbeat alive, will retry.");
                            lastSeenValue = existingValue;
                        } else {
                            LOGGER.info("Grabbing the lock since lock holder has failed to update its heartbeat");
                            expected = ImmutableList.of(existingValue);
                        }
                    }

                    long timeToSleep = CassandraConstants.TIME_BETWEEN_LOCK_ATTEMPT_ROUNDS_MILLIS;
                    Thread.sleep(timeToSleep);
                    casResult = writeDdlLockWithCas(client, expected, ourUpdate);
                }

                // we won the lock!
                LOGGER.info("Successfully acquired schema mutation lock.");
                return null;
            });
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
        return perOperationNodeId;
    }

    private void waitForSchemaMutationLockWithoutCas() throws TimeoutException {
        String message = "AtlasDB was unable to get a lock on Cassandra system schema mutations"
                + " for your cluster. Likely cause: Service(s) performing heavy schema mutations"
                + " in parallel, or extremely heavy Cassandra cluster load.";
        try {
            if (!schemaMutationLockForEarlierVersionsOfCassandra.tryLock(
                    configManager.getConfig().schemaMutationTimeoutMillis(),
                    TimeUnit.MILLISECONDS)) {
                throw new TimeoutException(message);
            }
        } catch (InterruptedException e) {
            throw new TimeoutException(message);
        }
    }

    private void schemaMutationUnlock(long perOperationNodeId) {
        try {
            clientPool.runWithRetry((FunctionCheckedException<Cassandra.Client, Void, Exception>) client -> {
                long heartbeatCount = heartbeatThread.heartbeatCount;
                List<Column> ourExpectedLock = ImmutableList.of(lockColumnWithStrValue(String.format(
                        CassandraConstants.GLOBAL_DDL_LOCK_FORMAT, perOperationNodeId, heartbeatCount)));
                Column clearedLock = lockColumnWithStrValue(CassandraConstants.GLOBAL_DDL_LOCK_CLEARED_VALUE);
                CASResult casResult = writeDdlLockWithCas(client, ourExpectedLock, clearedLock);

                if (!casResult.isSuccess()) {
                    handleForcedLockClear(casResult, perOperationNodeId, heartbeatCount);
                }
                LOGGER.info("Successfully released schema mutation lock.");
                return null;
            });
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private void schemaMutationUnlockWithoutCas() {
        schemaMutationLockForEarlierVersionsOfCassandra.unlock();
    }

    private void handleForcedLockClear(CASResult casResult, long perOperationNodeId, long heartbeatCount) {
        String remoteLock = "(unknown)";
        if (casResult.getCurrent_valuesSize() == 1) {
            Column column = Iterables.getOnlyElement(casResult.getCurrent_values(), null);
            if (column != null) {
                String remoteValue = new String(column.getValue(), StandardCharsets.UTF_8);
                long remoteId = Long.parseLong(remoteValue.split("_")[0]);
                remoteLock = (remoteId == CassandraConstants.GLOBAL_DDL_LOCK_CLEARED_ID)
                        ? "(Cleared Value)"
                        : remoteValue;
            }
        }

        String expectedLock = String.format(CassandraConstants.GLOBAL_DDL_LOCK_FORMAT, perOperationNodeId,
                heartbeatCount);
        throw new IllegalStateException(String.format("Another process cleared our schema mutation lock from"
                + " underneath us. Our ID, which we expected, was %s, the value we saw in the database was instead %s.",
                expectedLock, remoteLock));
    }

    private CASResult writeDdlLockWithCas(
            Cassandra.Client client,
            List<Column> expectedLockValue,
            Column newLockValue) throws TException {
        return writeLockWithCas(client, CassandraConstants.GLOBAL_DDL_LOCK_ROW_NAME, expectedLockValue, newLockValue);
    }

    private CASResult writeLockWithCas(
            Cassandra.Client client,
            ByteBuffer rowName,
            List<Column> expectedLockValue,
            Column newLockValue) throws TException {
        return client.cas(
                rowName,
                lockTable.getOnlyTable().getQualifiedName(),
                expectedLockValue,
                ImmutableList.of(newLockValue),
                ConsistencyLevel.SERIAL,
                writeConsistency
        );
    }

    private Column lockColumnWithStrValue(String strValue) {
        return lockColumnWithValue(strValue.getBytes(StandardCharsets.UTF_8));
    }

    private Column lockColumnWithValue(byte[] value) {
        return new Column()
                .setName(CassandraKeyValueServices.makeCompositeBuffer(
                        CassandraConstants.GLOBAL_DDL_LOCK_COLUMN_NAME.getBytes(StandardCharsets.UTF_8),
                        AtlasDbConstants.TRANSACTION_TS).array())
                .setValue(value) // expected previous
                .setTimestamp(AtlasDbConstants.TRANSACTION_TS);
    }
}
