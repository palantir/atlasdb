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

import com.google.common.base.Stopwatch;
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

    public class ActionWithHeartbeat {
        public volatile long heartbeatCounter;
        private final Action action;
        private final Thread heartbeatThread;
        private final long lockId;

        public ActionWithHeartbeat(Action action, long lockId) {
            this.action = action;
            this.heartbeatThread = new Thread(this::heartbeat);
            this.heartbeatThread.setName(Thread.currentThread().getName() + "-Heartbeat");
            this.lockId = lockId;
            this.heartbeatCounter = 0;
        }

        public void execute() throws Exception {
            if (!supportsCas) {
                // no heartbeat for non-Cas locks
                this.action.execute();
            } else {
                this.heartbeatThread.start();
                try {
                    this.action.execute();
                } finally {
                    this.heartbeatThread.interrupt();
                    this.heartbeatThread.join();
                }
            }
        }

        private void heartbeat() {
            try {
                while (true) {
                    clientPool.runWithRetry((FunctionCheckedException<Cassandra.Client, Void, Exception>) client -> {
                        Column ourUpdate = lockColumnWithStrValue(String.format(
                                CassandraConstants.GLOBAL_DDL_LOCK_FORMAT, lockId, heartbeatCounter + 1));

                        List<Column> expected = ImmutableList.of(lockColumnWithStrValue(String.format(
                                CassandraConstants.GLOBAL_DDL_LOCK_FORMAT, lockId, heartbeatCounter)));

                        CASResult casResult = writeDdlLockWithCas(client, expected, ourUpdate);
                        if (!casResult.isSuccess()) {
                            handleForcedLockClear(casResult, this.lockId);
                        } else {
                            heartbeatCounter++;
                        }
                        return null;
                    });
                    Thread.sleep(CassandraConstants.HEARTBEAT_TIME_PERIOD_MILLIS);
                }
            } catch (InterruptedException e) {
                // action complete, kill heartbeat
            } catch (Exception e) {
                throw Throwables.throwUncheckedException(e);
            }
        }
    }

    public void runWithLock(Action action) {
        long lockId = waitForSchemaMutationLock();
        ActionWithHeartbeat actionWithHeartbeat = new ActionWithHeartbeat(action, lockId);
        try {
            actionWithHeartbeat.execute();
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        } finally {
            schemaMutationUnlock(lockId, actionWithHeartbeat.heartbeatCounter);
        }
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
        final long perOperationNodeIdentifier = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE - 2);

        try {
            if (!supportsCas) {
                LOGGER.info("Because your version of Cassandra does not support check and set,"
                        + " we will use a java level lock to synchronise schema mutations."
                        + " If this is a clustered service, this could lead to corruption.");
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
                return 0;
            }

            clientPool.runWithRetry((FunctionCheckedException<Cassandra.Client, Void, Exception>) client -> {
                Column ourUpdate = lockColumnWithStrValue(
                        String.format(CassandraConstants.GLOBAL_DDL_LOCK_FORMAT, perOperationNodeIdentifier, 0));

                List<Column> expected = ImmutableList.of(lockColumnWithStrValue(
                        CassandraConstants.GLOBAL_DDL_LOCK_CLEARED_VALUE));

                CASResult casResult = writeDdlLockWithCas(client, expected, ourUpdate);

                int timesAttempted = 0;

                Stopwatch stopwatch = Stopwatch.createStarted();
                int mutationTimeoutMillis = configManager.getConfig().schemaMutationTimeoutMillis();
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
                        if (lastSeenValue == null || existingValue != lastSeenValue) {
                            lastSeenValue = existingValue;
                        } else {
                            LOGGER.info("Grabbing the lock since lock holder has failed to update its heartbeat");
                            expected = ImmutableList.of(existingValue);
                        }
                    }

//                    // possibly dead remote locker
//                    if (stopwatch.elapsed(TimeUnit.MILLISECONDS) > mutationTimeoutMillis * 4) {
//                        TimeoutException schemaLockTimeoutError = generateSchemaLockTimeoutException(stopwatch);
//                        LOGGER.error(schemaLockTimeoutError.getMessage(), schemaLockTimeoutError);
//                        throw Throwables.rewrapAndThrowUncheckedException(schemaLockTimeoutError);
//                    }

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

        return perOperationNodeIdentifier;
    }

    private TimeoutException generateSchemaLockTimeoutException(Stopwatch stopwatch) {
        return new TimeoutException(String.format("We have timed out waiting on the current"
                        + " schema mutation lock holder. We have tried to grab the lock for %d milliseconds"
                        + " unsuccessfully. This indicates that the current lock holder has died without"
                        + " releasing the lock and will require manual intervention. Shut down all AtlasDB"
                        + " clients and then run the clean-cass-locks-state cli command.",
                stopwatch.elapsed(TimeUnit.MILLISECONDS)));
    }

    private void schemaMutationUnlock(long perOperationNodeId, long heartbeatCounter) {
        if (!supportsCas) {
            schemaMutationLockForEarlierVersionsOfCassandra.unlock();
            return;
        }

        try {
            clientPool.runWithRetry((FunctionCheckedException<Cassandra.Client, Void, Exception>) client -> {
                List<Column> ourExpectedLock = ImmutableList.of(lockColumnWithStrValue(String.format(
                        CassandraConstants.GLOBAL_DDL_LOCK_FORMAT, perOperationNodeId, heartbeatCounter)));
                Column clearedLock = lockColumnWithStrValue(CassandraConstants.GLOBAL_DDL_LOCK_CLEARED_VALUE);
                CASResult casResult = writeDdlLockWithCas(client, ourExpectedLock, clearedLock);

                if (!casResult.isSuccess()) {
                    handleForcedLockClear(casResult, perOperationNodeId);
                }

                LOGGER.info("Successfully released schema mutation lock.");
                return null;
            });
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private void handleForcedLockClear(CASResult casResult, long perOperationNodeId) {
        String remoteLock = "(unknown)";
        if (casResult.getCurrent_valuesSize() == 1) {
            Column column = Iterables.getOnlyElement(casResult.getCurrent_values(), null);
            if (column != null) {
                String remoteId = new String(column.getValue(), StandardCharsets.UTF_8).split("_")[0];
                remoteLock = (Long.parseLong(remoteId) == CassandraConstants.GLOBAL_DDL_LOCK_CLEARED_ID)
                        ? "(Cleared Value)"
                        : remoteId;
            }
        }
        throw new IllegalStateException(String.format("Another process cleared our schema mutation lock"
                + " from underneath us. Our ID, which we expected, was %s, the value we saw in the"
                + " database was instead %s.", Long.toString(perOperationNodeId), remoteLock));
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
