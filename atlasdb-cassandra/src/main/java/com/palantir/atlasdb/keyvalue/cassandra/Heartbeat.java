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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.TException;

import com.google.common.collect.ImmutableList;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;

class Heartbeat implements Runnable {
    private final CassandraClientPool clientPool;
    private final AtomicInteger heartbeatCount;
    private final String lockTableName;
    private final ConsistencyLevel writeConsistency;
    private final long lockId;

    Heartbeat(CassandraClientPool clientPool,
              AtomicInteger heartbeatCount,
              String lockTableName,
              ConsistencyLevel writeConsistency,
              long lockId) {
        this.clientPool = clientPool;
        this.heartbeatCount = heartbeatCount;
        this.lockTableName = lockTableName;
        this.writeConsistency = writeConsistency;
        this.lockId = lockId;
    }

    @Override
    public void run() {
        try {
            clientPool.runWithRetry((FunctionCheckedException<Cassandra.Client, Void, TException>) client -> {
                Column ourUpdate = SchemaMutationLock.lockColumnFromIdAndHeartbeat(lockId, heartbeatCount.get() + 1);

                List<Column> expected = ImmutableList.of(SchemaMutationLock.lockColumnFromIdAndHeartbeat(
                        lockId, heartbeatCount.get()));

                if (Thread.currentThread().isInterrupted()) {
                    return null;
                }

                CASResult casResult = client.cas(CassandraConstants.GLOBAL_DDL_LOCK_ROW_NAME, lockTableName, expected,
                        ImmutableList.of(ourUpdate), ConsistencyLevel.SERIAL, writeConsistency);

                if (casResult.isSuccess()) {
                    heartbeatCount.incrementAndGet();
                } else {
                    SchemaMutationLock.handleForcedLockClear(casResult, lockId, heartbeatCount.get());
                }
                return null;
            });
        } catch (TException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }
}
