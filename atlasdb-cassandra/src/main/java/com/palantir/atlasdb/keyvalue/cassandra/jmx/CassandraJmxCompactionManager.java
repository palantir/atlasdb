/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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

import java.lang.reflect.UndeclaredThrowableException;
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
import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.TableReference;

/**
 * CassandraJmxCompactionManager manages all JMX compaction clients for C* cluster.
 * Each node in C* cluster will have one CassandraJmxCompactionClient connection.
 */
public final class CassandraJmxCompactionManager {
    private static final Logger log = LoggerFactory.getLogger(CassandraJmxCompactionManager.class);
    private final ImmutableSet<CassandraJmxCompactionClient> clients;
    private final ExecutorService exec;

    private CassandraJmxCompactionManager(ImmutableSet<CassandraJmxCompactionClient> clients, ExecutorService exec) {
        this.clients = Preconditions.checkNotNull(clients, "clients cannot be null");
        this.exec = Preconditions.checkNotNull(exec, "exec cannot be null");
    }

    public static CassandraJmxCompactionManager create(
            ImmutableSet<CassandraJmxCompactionClient> clients,
            ExecutorService exec) {
        return new CassandraJmxCompactionManager(clients, exec);
    }

    public void performTombstoneCompaction(long timeoutInSeconds,
                                           String keyspace,
                                           TableReference tableRef) throws InterruptedException, TimeoutException {
        Stopwatch stopWatch = Stopwatch.createStarted();
        if (!removeHintedHandoff(timeoutInSeconds)) {
            return;
        }
        log.info("All hinted handoff deletion tasks are completed.");

        long elapsedSeconds = stopWatch.elapsed(TimeUnit.SECONDS);
        long remainingTimeoutSeconds = timeoutInSeconds - elapsedSeconds;
        if (remainingTimeoutSeconds <= 0) {
            throw new TimeoutException(String.format("Task execution timeout in %d seconds. Timeout seconds:%d.",
                    elapsedSeconds, timeoutInSeconds));
        }

        // ALL HINTED HANDOFFS NEED TO BE DELETED BEFORE MOVING TO TOMBSTONE COMPACTION TASK
        if (!deleteTombstone(keyspace, tableRef, remainingTimeoutSeconds)) {
            return;
        }
        log.info("All compaction tasks are completed.");
    }

    private boolean removeHintedHandoff(long timeoutInSeconds) throws InterruptedException, TimeoutException {
        List<HintedHandOffDeletionTask> hintedHandoffDeletionTasks = Lists.newArrayListWithExpectedSize(clients.size());
        for (CassandraJmxCompactionClient client : clients) {
            hintedHandoffDeletionTasks.add(new HintedHandOffDeletionTask(client));
        }

        return executeInParallel(hintedHandoffDeletionTasks, timeoutInSeconds);
    }

    private boolean deleteTombstone(String keyspace, TableReference tableRef, long timeoutInSeconds)
            throws InterruptedException, TimeoutException {
        List<TombstoneCompactionTask> compactionTasks = Lists.newArrayListWithExpectedSize(clients.size());
        for (CassandraJmxCompactionClient client : clients) {
            compactionTasks.add(new TombstoneCompactionTask(client, keyspace, tableRef));
        }

        return executeInParallel(compactionTasks, timeoutInSeconds);
    }

    private boolean executeInParallel(List<? extends Callable<Void>> tasks, long timeoutInSeconds)
            throws InterruptedException, TimeoutException {
        Stopwatch stopWatch = Stopwatch.createStarted();
        List<Future<Void>> futures = exec.invokeAll(tasks, timeoutInSeconds, TimeUnit.SECONDS);

        for (Future<Void> f : futures) {
            if (f.isCancelled()) {
                log.error("Task execution timeouts in {} seconds. Timeout seconds: {}.",
                        stopWatch.stop().elapsed(TimeUnit.SECONDS), timeoutInSeconds);
                throw new TimeoutException(String.format("Task execution timeouts in %d seconds. Timeout seconds: %d.",
                        stopWatch.elapsed(TimeUnit.SECONDS), timeoutInSeconds));
            }

            try {
                f.get();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof UndeclaredThrowableException) {
                    log.error("Major LCS compactions are only supported against C* 2.2+; "
                            + "you will need to manually re-arrange SSTables into L0 "
                            + "if you want all deleted data immediately removed from the cluster.");
                }
                log.error("Failed to complete tasks.", e);
                return false;
            }
        }

        log.info("All tasks completed in {}.", stopWatch.stop());
        return true;
    }

    public String getCompactionStatus() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Jmx compaction information. Total number of Jmx clients: %d.%n", clients.size()));
        for (CassandraJmxCompactionClient client : clients) {
            sb.append(String.format("%s compaction summary: %s%n", client, client.getCompactionStatus()));
        }
        return sb.toString();
    }

    /**
     * Close all the JMX compaction connections.
     */
    public void close() {
        log.info("shutting down...");
        exec.shutdown();
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
