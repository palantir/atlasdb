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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.concurrent.PTExecutors;

final class HeartbeatService {
    private static final Logger log = LoggerFactory.getLogger(HeartbeatService.class);

    private final CassandraClientPool clientPool;
    private final TracingQueryRunner queryRunner;
    private final int heartbeatTimePeriodMillis;
    private final TableReference lockTable;
    private final ConsistencyLevel writeConsistency;
    private final ScheduledExecutorService heartbeatExecutorService;
    private final AtomicInteger heartbeatCount;

    static HeartbeatService create(
            CassandraClientPool clientPool,
            TracingQueryRunner queryRunner,
            int heartbeatTimePeriodMillis,
            TableReference lockTable,
            ConsistencyLevel writeConsistency) {
        ScheduledExecutorService executorService = PTExecutors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("Atlas Schema Lock Heartbeat-" + lockTable + "-%d").build());
        return new HeartbeatService(clientPool, queryRunner, heartbeatTimePeriodMillis, lockTable,
                writeConsistency, executorService);
    }

    HeartbeatService(CassandraClientPool clientPool,
            TracingQueryRunner queryRunner,
            int heartbeatTimePeriodMillis,
            TableReference lockTable,
            ConsistencyLevel writeConsistency,
            ScheduledExecutorService executorService) {
        this.clientPool = clientPool;
        this.queryRunner = queryRunner;
        this.heartbeatTimePeriodMillis = heartbeatTimePeriodMillis;
        this.lockTable = lockTable;
        this.writeConsistency = writeConsistency;
        this.heartbeatCount = new AtomicInteger(0);
        this.heartbeatExecutorService = executorService;
    }

    void startBeatingForLock(long lockId) {
        Preconditions.checkState(heartbeatCount.get() == 0 && !heartbeatExecutorService.isShutdown(),
                "Can't start new heartbeat with an existing heartbeat. Only one heartbeat per lock allowed.");

        heartbeatCount.set(0);
        heartbeatExecutorService.scheduleAtFixedRate(
                new Heartbeat(clientPool, queryRunner, heartbeatCount, lockTable, writeConsistency, lockId),
                0, heartbeatTimePeriodMillis, TimeUnit.MILLISECONDS);
    }

    void stopBeating() {
        if (heartbeatExecutorService.isShutdown()) {
            log.warn("HeartbeatService is already stopped");
            return;
        }

        heartbeatExecutorService.shutdown();
        try {
            long waitTime = 10L * heartbeatTimePeriodMillis;
            if (!heartbeatExecutorService.awaitTermination(waitTime, TimeUnit.MILLISECONDS)) {
                heartbeatExecutorService.shutdownNow();
                if (!heartbeatExecutorService.awaitTermination(waitTime, TimeUnit.MILLISECONDS)) {
                    throw new RuntimeException("Could not kill heartbeat");
                }
            }
        } catch (InterruptedException e) {
            heartbeatExecutorService.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            heartbeatExecutorService.shutdownNow();
        }
    }

    int getHeartbeatCount() {
        return heartbeatCount.get();
    }

}
