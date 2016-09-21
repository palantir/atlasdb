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

import com.google.common.base.Preconditions;
import com.palantir.common.concurrent.PTExecutors;

class HeartbeatService {
    private final CassandraClientPool clientPool;
    private final int heartbeatTimePeriodMillis;
    private final String lockTableName;
    private final ConsistencyLevel writeConsistency;
    private final AtomicInteger heartbeatCount;
    private ScheduledExecutorService heartbeatExecutorService;

    static final String startBeatingError = "Can't start new heartbeat with an existing heartbeat."
            + " Only one heartbeat per lock allowed.";
    static final String stopBeatingError = "Can't stop non existent heartbeat.";

    HeartbeatService(CassandraClientPool clientPool,
                     int heartbeatTimePeriodMillis,
                     String lockTableName,
                     ConsistencyLevel writeConsistency) {
        this.clientPool = clientPool;
        this.heartbeatTimePeriodMillis = heartbeatTimePeriodMillis;
        this.lockTableName = lockTableName;
        this.writeConsistency = writeConsistency;
        this.heartbeatCount = new AtomicInteger(0);
    }

    void startBeatingForLock(long lockId) {
        Preconditions.checkState(heartbeatExecutorService == null, startBeatingError);

        heartbeatExecutorService = PTExecutors.newSingleThreadScheduledExecutor();
        this.heartbeatCount.set(0);

        Heartbeat heartbeat = new Heartbeat(clientPool, heartbeatCount, lockTableName, writeConsistency, lockId);
        heartbeatExecutorService.scheduleAtFixedRate(heartbeat, 0, heartbeatTimePeriodMillis, TimeUnit.MILLISECONDS);
    }

    void stopBeating() {
        Preconditions.checkState(heartbeatExecutorService != null, stopBeatingError);

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

    int getCurrentHeartbeatCount() {
        return heartbeatCount.get();
    }
}
