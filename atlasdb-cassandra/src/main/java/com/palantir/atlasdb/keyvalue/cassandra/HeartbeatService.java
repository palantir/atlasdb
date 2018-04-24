/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.concurrent.PTExecutors;


public class HeartbeatService {
    private static final Logger log = LoggerFactory.getLogger(HeartbeatService.class);

    public static final String START_BEATING_ERR_MSG = "Can't start new heartbeat because there is an"
            + " existing heartbeat. Only one heartbeat per lock allowed.";
    public static final String STOP_BEATING_WARN_MSG = "HeartbeatService is already stopped";
    public static final int DEFAULT_HEARTBEAT_TIME_PERIOD_MILLIS = 1000;

    private final CassandraClientPool clientPool;
    private final TracingQueryRunner queryRunner;
    private final int heartbeatTimePeriodMillis;
    private final TableReference lockTable;
    private final ConsistencyLevel writeConsistency;
    private ScheduledExecutorService heartbeatExecutorService;

    public HeartbeatService(
            CassandraClientPool clientPool,
            TracingQueryRunner queryRunner,
            int heartbeatTimePeriodMillis,
            TableReference lockTable,
            ConsistencyLevel writeConsistency) {
        this.clientPool = clientPool;
        this.queryRunner = queryRunner;
        this.heartbeatTimePeriodMillis = heartbeatTimePeriodMillis;
        this.lockTable = lockTable;
        this.writeConsistency = writeConsistency;
        this.heartbeatExecutorService = null;
    }

    public synchronized void startBeatingForLock(long lockId) {
        Preconditions.checkState(heartbeatExecutorService == null, START_BEATING_ERR_MSG);
        heartbeatExecutorService = PTExecutors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("Atlas Schema Lock Heartbeat-" + lockTable + "-%d").build());
        heartbeatExecutorService.scheduleAtFixedRate(
                new Heartbeat(clientPool, queryRunner, lockTable, writeConsistency, lockId),
                0, heartbeatTimePeriodMillis, TimeUnit.MILLISECONDS);
    }

    public synchronized void stopBeating() {
        if (heartbeatExecutorService == null) {
            log.warn(STOP_BEATING_WARN_MSG);
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
            heartbeatExecutorService = null;
        }
    }
}
