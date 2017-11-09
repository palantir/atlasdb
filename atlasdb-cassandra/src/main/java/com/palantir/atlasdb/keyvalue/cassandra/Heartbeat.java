/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.Throwables;

class Heartbeat implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(Heartbeat.class);

    private final CassandraClientPool clientPool;
    private final TracingQueryRunner queryRunner;
    private final TableReference lockTable;
    private final ConsistencyLevel writeConsistency;
    private final long lockId;
    private int heartbeatCount;

    Heartbeat(CassandraClientPool clientPool,
              TracingQueryRunner queryRunner,
              TableReference lockTable,
              ConsistencyLevel writeConsistency,
              long lockId) {
        this.clientPool = clientPool;
        this.queryRunner = queryRunner;
        this.lockTable = lockTable;
        this.writeConsistency = writeConsistency;
        this.lockId = lockId;
        this.heartbeatCount = 0;
    }

    @Override
    public void run() {
        try {
            try {
                clientPool.runWithRetry(this::beat);
            } catch (TException e) {
                throw Throwables.throwUncheckedException(e);
            }
        } catch (Throwable throwable) {
            // Avoid letting heartbeat thread die
            log.error("Heartbeat threw unexpected exception {}", throwable, throwable);
        }
    }

    private Void beat(CassandraClient client) throws TException {
        Column ourUpdate = SchemaMutationLock.lockColumnFromIdAndHeartbeat(lockId, heartbeatCount + 1);

        List<Column> expected = ImmutableList.of(
                SchemaMutationLock.lockColumnFromIdAndHeartbeat(lockId, heartbeatCount));

        if (Thread.currentThread().isInterrupted()) {
            log.debug("Cancelled {}", this);
            return null;
        }

        CASResult casResult = writeDdlLockWithCas(client, ourUpdate, expected);
        if (casResult.isSuccess()) {
            heartbeatCount++;
        } else {
            log.warn("Unable to update lock for {}", this);
            SchemaMutationLock.handleForcedLockClear(casResult, lockId, heartbeatCount);
        }
        log.debug("Completed {}", this);
        return null;
    }

    private CASResult writeDdlLockWithCas(CassandraClient client, Column ourUpdate, List<Column> expected)
            throws TException {
        try {
            return queryRunner.run(client, lockTable,
                    () -> client.cas(
                            lockTable,
                            SchemaMutationLock.getGlobalDdlLockRowName(),
                            expected,
                            ImmutableList.of(ourUpdate),
                            ConsistencyLevel.SERIAL,
                            writeConsistency));
        } catch (UnavailableException e) {
            throw new InsufficientConsistencyException(
                    "CAS for the heartbeat requires " + writeConsistency + "Cassandra nodes to be available.", e);
        }
    }

    @Override
    public String toString() {
        return "Heartbeat{"
                + "lockId=" + lockId
                + ", lockTable='" + lockTable + '\''
                + ", writeConsistency=" + writeConsistency
                + ", heartbeatId=" + heartbeatCount
                + '}';
    }
}
