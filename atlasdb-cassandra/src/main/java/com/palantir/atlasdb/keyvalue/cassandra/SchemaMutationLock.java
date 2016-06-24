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

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Longs;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;

public class SchemaMutationLock {
    private boolean supportsCAS;

    private CassandraKeyValueServiceConfigManager configManager;
    private CassandraClientPool clientPool;
    private ConsistencyLevel writeConsistency;
    private final ReentrantLock schemaMutationLockForEarlierVersionsOfCassandra = new ReentrantLock(true);

    public SchemaMutationLock(boolean supportsCAS, CassandraKeyValueServiceConfigManager configManager, CassandraClientPool clientPool, ConsistencyLevel writeConsistency) {
        this.supportsCAS = supportsCAS;
        this.configManager = configManager;
        this.clientPool = clientPool;
        this.writeConsistency = writeConsistency;
    }

    public interface Action {
        void execute() throws Exception;
    }

    public void runWithLock(Action action) {
        long lockId = waitForSchemaMutationLock();

        try {
            action.execute();
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        } finally {
            schemaMutationUnlock(lockId);
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
     * I can't hold this against them though, because Atlas has a semi-similar problem with empty byte[] values meaning deleted internally.
     *
     * @return an ID to be passed into a subsequent unlock call
     */
    private long waitForSchemaMutationLock() {
        final long perOperationNodeIdentifier = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE - 2);

        try {
            if (!supportsCAS) {
                String message = "AtlasDB was unable to get a lock on Cassandra system schema mutations for your cluster. Likely cause: Service(s) performing heavy schema mutations in parallel, or extremely heavy Cassandra cluster load.";
                try {
                    if (!schemaMutationLockForEarlierVersionsOfCassandra.tryLock(configManager.getConfig().schemaMutationTimeoutMillis(), TimeUnit.MILLISECONDS)) {
                        throw new TimeoutException(message);
                    }
                } catch (InterruptedException e) {
                    throw new TimeoutException(message);
                }
                return 0;
            }

            clientPool.runWithRetry((FunctionCheckedException<Cassandra.Client, Void, Exception>) client -> {
                Cell globalDdlLockCell = Cell.create(CassandraConstants.GLOBAL_DDL_LOCK.getBytes(), CassandraConstants.GLOBAL_DDL_LOCK_COLUMN_NAME.getBytes());
                ByteBuffer rowName = ByteBuffer.wrap(globalDdlLockCell.getRowName());
                Column ourUpdate = lockColumnWithValue(Longs.toByteArray(perOperationNodeIdentifier));

                List<Column> expected = ImmutableList.of(lockColumnWithValue(Longs.toByteArray(CassandraConstants.GLOBAL_DDL_LOCK_CLEARED_VALUE)));

                CASResult casResult = writeLockWithCAS(client, rowName, expected, ourUpdate);

                int timesAttempted = 0;

                Stopwatch stopwatch = Stopwatch.createStarted();
                while (!casResult.isSuccess()) { // could have a timeout controlling this level, confusing for users to set both timeouts though

                    if (casResult.getCurrent_valuesSize() == 0) { // never has been an existing lock
                        // special case, no one has ever made a lock ever before
                        // this becomes analogous to putUnlessExists now
                        expected = ImmutableList.<Column>of();
                    } else {
                        Column existingValue = Iterables.getOnlyElement(casResult.getCurrent_values(), null);
                        if (existingValue == null) {
                            throw new IllegalStateException("Something is wrong with underlying locks. Consult support for guidance on manually examining and clearing locks from " + CassandraConstants.LOCK_TABLE + " table.");
                        }
                        expected = ImmutableList.of(lockColumnWithValue(Longs.toByteArray(CassandraConstants.GLOBAL_DDL_LOCK_CLEARED_VALUE)));
                    }

                    if (stopwatch.elapsed(TimeUnit.MILLISECONDS) > configManager.getConfig().schemaMutationTimeoutMillis() * 4) { // possibly dead remote locker
                        throw new TimeoutException(String.format("We have timed out waiting on the current schema mutation lock holder.  " +
                                "We have tried to grab the lock for %d milliseconds unsuccessfully.  Please try restarting the AtlasDB client." +
                                "If this occurs repeatedly it may indicate that the current lock holder has died without releasing the lock " +
                                "and will require manual intervention. This will require restarting all atlasDB clients and then using cqlsh " +
                                "to truncate the _locks table. Please contact support for help with this in important situations.", stopwatch.elapsed(TimeUnit.MILLISECONDS)));
                    }

                    long timeToSleep = CassandraConstants.TIME_BETWEEN_LOCK_ATTEMPT_ROUNDS_MILLIS * (long) Math.pow(2, timesAttempted++);

                    Thread.sleep(timeToSleep);

                    casResult = writeLockWithCAS(client, rowName, expected, ourUpdate);
                }

                // we won the lock!
                return null;
            });
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }

        return perOperationNodeIdentifier;
    }

    private void schemaMutationUnlock(long perOperationNodeIdentifier) {
        if (!supportsCAS) {
            schemaMutationLockForEarlierVersionsOfCassandra.unlock();
            return;
        }

        try {
            clientPool.runWithRetry((FunctionCheckedException<Cassandra.Client, Void, Exception>) client -> {
                Cell globalDdlLockCell = Cell.create(CassandraConstants.GLOBAL_DDL_LOCK.getBytes(), CassandraConstants.GLOBAL_DDL_LOCK_COLUMN_NAME.getBytes());
                ByteBuffer rowName = ByteBuffer.wrap(globalDdlLockCell.getRowName());

                List<Column> ourExpectedLock = ImmutableList.of(lockColumnWithValue(Longs.toByteArray(perOperationNodeIdentifier)));
                Column clearedLock = lockColumnWithValue(Longs.toByteArray(CassandraConstants.GLOBAL_DDL_LOCK_CLEARED_VALUE));
                CASResult casResult = writeLockWithCAS(client, rowName, ourExpectedLock, clearedLock);

                if (!casResult.isSuccess()) {
                    String remoteLock = "(unknown)";
                    if (casResult.getCurrent_valuesSize() == 1) {
                        Column column = Iterables.getOnlyElement(casResult.getCurrent_values(), null);
                        if (column != null) {
                            long remoteId = Longs.fromByteArray(column.getValue());
                            remoteLock = (remoteId == CassandraConstants.GLOBAL_DDL_LOCK_CLEARED_VALUE) ? "(Cleared Value)" : Long.toString(remoteId);
                        }
                    }
                    throw new IllegalStateException(String.format("Another process cleared our schema mutation lock from underneath us. Our ID, which we expected, was %s, the value we saw in the database was instead %s.", Long.toString(perOperationNodeIdentifier), remoteLock));
                }
                return null;
            });
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private CASResult writeLockWithCAS(Cassandra.Client client, ByteBuffer rowName, List<Column> expectedLockValue, Column newLockValue) throws TException {
        return client.cas(
                rowName,
                CassandraConstants.LOCK_TABLE.getQualifiedName(),
                expectedLockValue,
                ImmutableList.of(newLockValue),
                ConsistencyLevel.SERIAL,
                writeConsistency
        );
    }

    private Column lockColumnWithValue(byte[] value) {
        return new Column()
                .setName(CassandraKeyValueServices.makeCompositeBuffer(CassandraConstants.GLOBAL_DDL_LOCK_COLUMN_NAME.getBytes(), AtlasDbConstants.TRANSACTION_TS).array())
                .setValue(value) // expected previous
                .setTimestamp(AtlasDbConstants.TRANSACTION_TS);
    }
}
