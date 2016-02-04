/**
 * Copyright 2015 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra.jmx;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * CassandraJmxCompactionManager manages all JMX compaction clients for C* cluster.
 * Each node in C* cluster will have one CassandraJmxCompactionClient connection.
 */
public class CassandraJmxCompactionManager {
    private static final Logger log = LoggerFactory.getLogger(CassandraJmxCompactionManager.class);
    private final ImmutableSet<CassandraJmxCompactionClient> clients;
    private final ExecutorService exec;

    private CassandraJmxCompactionManager(ImmutableSet<CassandraJmxCompactionClient> clients, ExecutorService exec) {
        this.clients = Preconditions.checkNotNull(clients);
        this.exec = Preconditions.checkNotNull(exec);
    }

    public static CassandraJmxCompactionManager create(ImmutableSet<CassandraJmxCompactionClient> clients, ExecutorService exec) {
        return new CassandraJmxCompactionManager(clients, exec);
    }

    public void performTombstoneCompaction(long timeoutInSeconds,
                                           String keyspace,
                                           String tableName) throws InterruptedException, TimeoutException {
        Stopwatch stopWatch = Stopwatch.createStarted();
        removeHintedHandoff();
        long remainingTimeoutSeconds = timeoutInSeconds - stopWatch.elapsed(TimeUnit.SECONDS);

        // ALL hinted handoffs need to be deleted before moving to tombstone compaction task
        deleteTombStone(keyspace, tableName, remainingTimeoutSeconds);
    }

    private void removeHintedHandoff() throws InterruptedException {
        CassandraJmxCompactionClient client = Iterables.get(clients, 0);
        try {
            client.truncateAllHints();
        } catch (ExecutionException e) {
            log.error("Failed to remove hinted handoff.", e);
            return;
        }
        log.info("All hinted handoffs are deleted.");
    }

    private void deleteTombStone(String keyspace, String tableName, long timeoutInSeconds) throws InterruptedException, TimeoutException {
        List<TombStoneCompactionTask> compactionTasks = Lists.newArrayListWithExpectedSize(clients.size());
        for (CassandraJmxCompactionClient client : clients) {
            compactionTasks.add(new TombStoneCompactionTask(client, keyspace, tableName));
        }
        executeParallel(exec, compactionTasks, timeoutInSeconds);
        log.info("All compaction tasks are completed.");
    }

    private void executeParallel(ExecutorService exec, List<? extends Callable<Boolean>> tasks, long timeoutInSeconds)
            throws InterruptedException, TimeoutException {
        Stopwatch stopWatch = Stopwatch.createStarted();
        List<Future<Boolean>> futures = exec.invokeAll(tasks, timeoutInSeconds, TimeUnit.SECONDS);

        for (Future<Boolean> f : futures) {
            if (f.isCancelled()) {
                log.error("Task execution timeout in {} seconds. Timeout seconds:{}.", stopWatch.stop(), timeoutInSeconds);
                throw new TimeoutException(String.format("Task execution timeout in {} seconds. Timeout seconds:{}.",
                        stopWatch.stop(), timeoutInSeconds));

            }
        }
        log.info("All tasks completed in {}.", stopWatch.stop());
    }

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
        log.info("closing {} JMX clients.", clients.size());
        for (CassandraJmxCompactionClient client : clients) {
            client.close();
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("clients", clients).toString();
    }
}