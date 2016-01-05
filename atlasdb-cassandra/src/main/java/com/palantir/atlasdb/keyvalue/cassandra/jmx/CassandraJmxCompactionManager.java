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
package com.palantir.atlasdb.keyvalue.cassandra.jmx;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

/**
 * CassandraJmxCompactionManager manages all JMX compaction clients for C* cluster.
 * Each node in C* cluster will have one CassandraJmxCompactionClient connection.
 */
public class CassandraJmxCompactionManager {
    private static final Logger log = LoggerFactory.getLogger(CassandraJmxCompactionManager.class);
    private final Set<CassandraJmxCompactionClient> clients;
    private final ExecutorService exec;

    private CassandraJmxCompactionManager(Set<CassandraJmxCompactionClient> clients, ExecutorService exec) {
        this.clients = clients;
        this.exec = exec;
    }

    public static CassandraJmxCompactionManager newInstance(Set<CassandraJmxCompactionClient> clients, ExecutorService exec) {
        Preconditions.checkNotNull(clients);
        Preconditions.checkNotNull(exec);
        return new CassandraJmxCompactionManager(clients, exec);
    }

    public Set<CassandraJmxCompactionClient> getCompactionClients() {
        return Collections.unmodifiableSet(clients);
    }

    /**
     *
     * @param timeoutInSeconds - timeout for compaction.
     * @param keyspace - keyspace for the tables to be compacted
     * @param tableName - tables to be compacted
     * @throws TimeoutException - TimeoutException is thrown when compaction cannot finish in a given time period.
     * @throws InterruptedException
     */
    public void performTombstoneCompaction(final long timeoutInSeconds,
                                     final String keyspace,
                                     final String tableName) throws TimeoutException, InterruptedException {
        if(clients.isEmpty()) {
            log.error("No compaction client can be found.");
            return;
        }

        List<HintedHandoffDeletionTask> hintedHandoffDeletionTasks = Lists.newArrayListWithExpectedSize(clients.size());
        for (CassandraJmxCompactionClient client : clients) {
            hintedHandoffDeletionTasks.add(new HintedHandoffDeletionTask(client));
        }

        Stopwatch stopWatch = Stopwatch.createStarted();
        List<Future<Boolean>> hintedFutures = exec.invokeAll(hintedHandoffDeletionTasks, timeoutInSeconds, TimeUnit.SECONDS);
        for (Future<Boolean> f : hintedFutures) {
            if (f.isCancelled()) {
                throw new TimeoutException(String.format("Hinted handoff deletion timeout after %d seconds.", timeoutInSeconds));
            }
        }
        log.info("All hinted handoffs are deleted, consumed {}.", stopWatch.stop());

        // ALL hinted handoffs need to be deleted before moving to tombstone compaction task
        long remainingTimeoutSeconds = timeoutInSeconds - stopWatch.elapsed(TimeUnit.SECONDS);
        List<TombStoneCompactionTask> compactionTasks = Lists.newArrayListWithExpectedSize(clients.size());
        for (CassandraJmxCompactionClient client : clients) {
            compactionTasks.add(new TombStoneCompactionTask(client, keyspace, tableName));
        }

        stopWatch = Stopwatch.createStarted();
        List<Future<Boolean>> compactionFutures = exec.invokeAll(compactionTasks, remainingTimeoutSeconds, TimeUnit.SECONDS);

        for (Future<Boolean> f : compactionFutures) {
            if (f.isCancelled()) {
                throw new TimeoutException(String.format("Compaction for [%s].[%s] timeout after %d seconds.",
                        keyspace, tableName, remainingTimeoutSeconds));
            }
        }
        log.info("All compaction tasks for {}.{} completed, consumed {}.", keyspace, tableName, stopWatch.stop());
    }

    private static class HintedHandoffDeletionTask implements Callable<Boolean> {
        private final CassandraJmxCompactionClient client;

        HintedHandoffDeletionTask(CassandraJmxCompactionClient client){
            this.client = client;
        }

        @Override
        public Boolean call() throws Exception {
            client.deleteLocalHints();
            return true;
        }

    }

    private static class TombStoneCompactionTask implements Callable<Boolean> {
        private final CassandraJmxCompactionClient client;
        private final String keyspace;
        private final String tableName;

        TombStoneCompactionTask(CassandraJmxCompactionClient client, String keyspace, String tableName) {
            this.client = client;
            this.keyspace = keyspace;
            this.tableName = tableName;
        }

        @Override
        public Boolean call() throws Exception {
            // make sure tombstone are persisted on disk for tombstone compaction
            client.forceTableFlush(keyspace, tableName);
            client.forceTableCompaction(keyspace, tableName);
            return true;
        }
    }

    /**
     * remaining compaction tasks left on each node
     */
    public String getCompactionStatus() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Jmx compaction information. Total number of Jmx clients: %d.\n", clients.size()));
        for (CassandraJmxCompactionClient client : clients) {
            sb.append(String.format("%s compaction summary: %s\n", client, client.getCompactionStatus()));
        }
        return sb.toString();
    }

    /**
     * close all the JMX compaction connection
     */
    public void close() {
        log.info("closing Jmx compactions. Total number of JMX clients: {}.", clients.size());
        for (CassandraJmxCompactionClient client : clients) {
            client.close();
            log.info("compaction on {} closed.", client.getHost());
        }
    }

    @Override
    public String toString() {
        return "CassandraJmxCompactionManager [compactionClients=" + clients + "]";
    }
}