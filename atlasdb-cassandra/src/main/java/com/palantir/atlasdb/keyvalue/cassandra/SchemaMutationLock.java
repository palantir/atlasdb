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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
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
    private final HeartbeatService heartbeatService;

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
        this.heartbeatService = new HeartbeatService();
    }

    public interface Action {
        void execute() throws Exception;
    }

    private class Heartbeat implements Runnable {
        private final long lockId;
        private final AtomicInteger heartbeatCount;

        Heartbeat(long lockId, AtomicInteger heartbeatCount) {
            this.lockId = lockId;
            this.heartbeatCount = heartbeatCount;
        }

        @Override
        public void run() {
            try {
                clientPool.runWithRetry((FunctionCheckedException<Cassandra.Client, Void, TException>) client -> {
                    Column ourUpdate = lockColumnWithStrValue(
                            CassandraConstants.lockValueFromIdAndHeartbeat(lockId, heartbeatCount.get() + 1));

                    List<Column> expected = ImmutableList.of(lockColumnWithStrValue(
                            CassandraConstants.lockValueFromIdAndHeartbeat(lockId, heartbeatCount.get())));

                    if (Thread.currentThread().isInterrupted()) {
                        return null;
                    }

                    CASResult casResult = writeDdlLockWithCas(client, expected, ourUpdate);

                    if (casResult.isSuccess()) {
                        heartbeatCount.incrementAndGet();
                    } else {
                        handleForcedLockClear(casResult, lockId, heartbeatCount.get());
                    }
                    return null;
                });
            } catch (TException e) {
                throw Throwables.throwUncheckedException(e);
            }

        }
    }

    private class HeartbeatService {
        private ScheduledExecutorService heartbeatExecutorService;
        private AtomicInteger heartbeatCount;
        private final int heartbeatTimePeriodMillis;

        HeartbeatService() {
            heartbeatCount = new AtomicInteger(0);
            heartbeatTimePeriodMillis = configManager.getConfig().heartbeatTimePeriodMillis();
        }

        void startBeatingForLock(long lockId) {
            String err = "Can\'t start new heartbeat with an existing heartbeat. Only one heartbeat per lock allowed.";
            Preconditions.checkState(heartbeatExecutorService == null, err);

            heartbeatExecutorService = Executors.newSingleThreadScheduledExecutor();
            this.heartbeatCount.set(0);
            heartbeatExecutorService.scheduleAtFixedRate(new Heartbeat(lockId, heartbeatCount), 0,
                    heartbeatTimePeriodMillis, TimeUnit.MILLISECONDS);
        }

        void stopBeating() {
            String err = "Can\'t stop non existent heartbeat.";
            Preconditions.checkState(heartbeatExecutorService != null, err);
            heartbeatExecutorService.shutdown();
            try {
                if (!heartbeatExecutorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    heartbeatExecutorService.shutdownNow();
                    if (!heartbeatExecutorService.awaitTermination(10, TimeUnit.SECONDS)) {
                        throw new RuntimeException("Could not kill heartbeat");
                    }
                }
            } catch (InterruptedException e) {
                heartbeatExecutorService.shutdownNow();
                Thread.currentThread().interrupt();
            } finally {
                heartbeatExecutorService = null;
            }
        }
    }

    void runWithLock(Action action) {
        if (!supportsCas) {
            runWithLockWithoutCas(action);
            return;
        }

        long lockId = waitForSchemaMutationLock();
        heartbeatService.startBeatingForLock(lockId);
        try {
            action.execute();
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        } finally {
            heartbeatService.stopBeating();
            schemaMutationUnlock(lockId);
        }
    }

    private void runWithLockWithoutCas(Action action) {
        if (configManager.getConfig().servers().size() > 1) {
            throw new UnsupportedOperationException("Running a clustered service with a version of Cassandra"
                    + " that does not support check and set is not allowed. Either upgrade Cassandra or run"
                    + " a single node service");
        }
        LOGGER.info("Because your version of Cassandra does not support check and set,"
                + " we will use a java level lock to synchronise schema mutations.");
        try {
            waitForSchemaMutationLockWithoutCas();
            action.execute();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.throwUncheckedException(e);
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        } finally {
            schemaMutationUnlockWithoutCas();
        }

    }

    void killHeartbeat() {
        heartbeatService.stopBeating();
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
                Column ourUpdate = lockColumnWithStrValue(CassandraConstants.lockValueFromIdAndHeartbeat(
                        perOperationNodeId, 0));

                List<Column> expected = ImmutableList.of(lockColumnWithStrValue(
                        CassandraConstants.GLOBAL_DDL_LOCK_CLEARED_VALUE));

                CASResult casResult = writeDdlLockWithCas(client, expected, ourUpdate);

                Column lastSeenValue = null;
                int timesAttempted = 0;

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

                    long timeToSleep = CassandraConstants.TIME_BETWEEN_LOCK_ATTEMPT_ROUNDS_MILLIS
                            * (long) Math.pow(2, timesAttempted++);
                    Thread.sleep(timeToSleep);
                    casResult = writeDdlLockWithCas(client, expected, ourUpdate);
                }

                // we won the lock!
                LOGGER.info("Successfully acquired schema mutation lock.");
                return null;
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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
            Thread.currentThread().interrupt();
            throw new TimeoutException(message);
        }
    }

    private void schemaMutationUnlock(long perOperationNodeId) {
        try {
            clientPool.runWithRetry((FunctionCheckedException<Cassandra.Client, Void, TException>) client -> {
                int heartbeatCount = heartbeatService.heartbeatCount.get();
                Column lockColumn = lockColumnWithStrValue(
                        CassandraConstants.lockValueFromIdAndHeartbeat(perOperationNodeId, heartbeatCount));
                List<Column> ourExpectedLock = ImmutableList.of(lockColumn);
                Column clearedLock = lockColumnWithStrValue(CassandraConstants.GLOBAL_DDL_LOCK_CLEARED_VALUE);
                CASResult casResult = writeDdlLockWithCas(client, ourExpectedLock, clearedLock);

                if (!casResult.isSuccess()) {
                    handleForcedLockClear(casResult, perOperationNodeId, heartbeatCount);
                }
                LOGGER.info("Successfully released schema mutation lock.");
                return null;
            });
        } catch (TException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private void schemaMutationUnlockWithoutCas() {
        schemaMutationLockForEarlierVersionsOfCassandra.unlock();
    }

    private void handleForcedLockClear(CASResult casResult, long perOperationNodeId, int heartbeatCount) {
        Preconditions.checkState(casResult.getCurrent_valuesSize() == 1,
                "Something is wrong with the underlying locks. Consult support for guidance.");

        String remoteLock = "(unknown)";
        Column column = Iterables.getOnlyElement(casResult.getCurrent_values(), null);
        if (column != null) {
            String remoteValue = new String(column.getValue(), StandardCharsets.UTF_8);
            long remoteId = Long.parseLong(remoteValue.split("_")[0]);
            remoteLock = (remoteId == CassandraConstants.GLOBAL_DDL_LOCK_CLEARED_ID)
                    ? "(Cleared Value)"
                    : remoteValue;
        }

        String expectedLock = CassandraConstants.lockValueFromIdAndHeartbeat(perOperationNodeId, heartbeatCount);
        throw new IllegalStateException(String.format("Another process cleared our schema mutation lock from"
                + " underneath us. Our ID, which we expected, was %s, the value we saw in the database"
                + " was instead %s.", expectedLock, remoteLock));
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
