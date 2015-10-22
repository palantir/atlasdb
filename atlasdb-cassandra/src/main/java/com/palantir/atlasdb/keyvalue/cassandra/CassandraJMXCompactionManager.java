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
package com.palantir.atlasdb.keyvalue.cassandra;

import java.io.File;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.cassandra.CassandraJmxCompactionConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;

/**
 * CassandraJMXCompactionManager manages all JMX compactions for C* cluster.
 * Each node in C* cluster will have one CassandraJMXCompaction connection.
 */
public final class CassandraJMXCompactionManager {
    private static final Logger log = LoggerFactory.getLogger(CassandraJMXCompactionManager.class);
    private final Set<CassandraJMXCompaction> compactionClients;

    private CassandraJMXCompactionManager() {
        this.compactionClients = Sets.newHashSetWithExpectedSize(0);
    }

    /**
     * @param config
     * @return an empty compactionManager without any compaction client when JMX is not enabled
     */
    public static CassandraJMXCompactionManager newInstance(CassandraKeyValueServiceConfig config) {
        Preconditions.checkNotNull(config);
        CassandraJMXCompactionManager compactionManager = new CassandraJMXCompactionManager();
        // if JMX is not enabled, return the empty compactionManager
        if (!config.jmx().isPresent()) {
            return compactionManager;
        }
        // need to set the property before creating the JMX compaction client
        setProperty(config.jmx().get());

        compactionManager.initCompactionClients(config);
        return compactionManager;
    }

    private void initCompactionClients(CassandraKeyValueServiceConfig config){
        Preconditions.checkNotNull(config);

        Set<String> hosts = config.servers();
        Preconditions.checkState(!hosts.isEmpty(), "address list should not be empty");
        String jmxUsername = config.jmx().get().username();
        String jmxPassword = config.jmx().get().password();
        int jmxPort = config.jmx().get().port();
        boolean isJmxSslEnabled = config.jmx().get().ssl();

        for (String host : hosts) {
            CassandraJMXCompaction client = null;
            if (isJmxSslEnabled) {
                client = new CassandraJMXCompaction.Builder(host, jmxPort).username(jmxUsername).password(jmxPassword).build();
            } else {
                client = new CassandraJMXCompaction.Builder(host, jmxPort).build();
            }
            if (client != null) {
                compactionClients.add(client);
            }
        }
    }

    public Set<CassandraJMXCompaction> getCompactionClients() {
        return compactionClients;
    }

    /**
     * Running compaction against C* without SSL enabled experiences
     * some hanging tcp connection during compaction processes. So
     *         System.setProperty("sun.rmi.transport.tcp.responseTimeout", String.valueOf(jmxRmiTimeoutMillis));
     * will enforce the tcp connection timeout in case it happens.
     *
     * @param config
     */
    private static void setProperty(CassandraJmxCompactionConfig config) {
        long jmxRmiTimeoutMillis = config.jmxRmiTimeoutMillis();
        // NOTE: RMI timeout to avoid hanging tcp connection
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", String.valueOf(jmxRmiTimeoutMillis));

        String keyStoreFile = config.keystore();
        String keyStorePassword = config.keystorePassword();
        String trustStoreFile = config.truststore();
        String trustStorePassword = config.truststorePassword();
        Preconditions.checkState((new File(keyStoreFile)).exists(), "file:" + keyStoreFile + " doest not exist!");
        Preconditions.checkState((new File(trustStoreFile)).exists(), "file:" + trustStoreFile + " doest not exist!");
        System.setProperty("javax.net.ssl.keyStore", keyStoreFile);
        System.setProperty("javax.net.ssl.keyStorePassword", keyStorePassword);
        System.setProperty("javax.net.ssl.trustStore", trustStoreFile);
        System.setProperty("javax.net.ssl.trustStorePassword", trustStorePassword);
    }

    /**
     * A thread pool with #(nodes) threads will be created to wait for compaction to complete.
     *
     * @param timeout - timeout for compaction. After timeout, the compaction task will not be canceled.
     * @param keyspace - keyspace for the tables to be compacted
     * @param tableName - tables to be compacted
     * @throws TimeoutException - TimeoutException is thrown when compaction cannot finish in a given time period.
     */
    public void forceTableCompaction(long timeout, final String keyspace, final String tableName) throws TimeoutException {
        if (compactionClients.isEmpty()) {
            log.error("No compaction client found in CassandraJMXCompactionManager, cannot perform compaction on {}.{}.", keyspace, tableName);
            log.error("Follow steps as follows to use Cassandra JMX compaction feature:");
            log.error("1. Enable JMX options in `cassandra-env.sh`, prefer to use SSL authentication option.");
            log.error("2. Add \"jmx\"=true in ATLAS_KVS_PREFERENCES.");
            return;
        }

        // gc_grace_period is turned off. Now delete all hintedHandoffs
        // hintedHandoffs need to be deleted on all nodes before running Cassandra compaction
        for (CassandraJMXCompaction compaction : compactionClients) {
            compaction.deleteLocalHints();
        }

        // execute the compaction to remove the tombstones
        // the reason to create a threadpool every time this function is called is because we need to call shutdown()
        // to make awaitTermination() return if tasks complete before timeout.
        ExecutorService exec = Executors.newFixedThreadPool(
                compactionClients.size(),
                new ThreadFactoryBuilder().setNameFormat("CassandraCompactionThreadPool-%d").build());
        Stopwatch timer = Stopwatch.createStarted();
        for (final CassandraJMXCompaction compaction: compactionClients) {
            exec.submit(new Runnable() {
                @Override
                public void run() {
                    compaction.forceTableFlush(keyspace, tableName);
                    compaction.forceTableCompaction(keyspace, tableName);
                }
            });
        }
        // shutdown makes exec.awaitTermination() return if job completes before timeout
        exec.shutdown();
        try {
            boolean isExecuted = exec.awaitTermination(timeout, TimeUnit.SECONDS);
            if (isExecuted) {
                log.info("All compaction tasks for {}.{} consumed {}", keyspace, Arrays.asList(tableName), timer.stop());
            } else {
                exec.shutdownNow();
                throw new TimeoutException(String.format("Compaction timeout for %s:%s in %d seconds", keyspace, Arrays.asList(tableName), timeout));
            }
        } catch (InterruptedException e) {
            log.error("Waiting for compaction is interupted for {}.{}. Error: {}", keyspace, Arrays.asList(tableName), e.getMessage());
        }
    }

    /**
     * remaining compaction tasks left on each node
     */
    public String getPendingCompactionStatus() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("\nJMX compaction information. Total number of JMX compaction: %d\n", compactionClients.size()));
        for (CassandraJMXCompaction compaction: compactionClients) {
            sb.append(String.format("%s has %d pending compaction tasks\n", compaction.getHost(), compaction.getPendingTasks()));
        }
        return sb.toString();
    }

    /**
     * close all the JMX compaction connection
     */
    public void close() {
        log.info("closing JMX compactions. Total number of JMX compaction: {}", compactionClients.size());
        for (CassandraJMXCompaction compaction: compactionClients) {
            compaction.close();
            log.info("compaction on {} closed", compaction.getHost());
        }
    }

    @Override
    public String toString() {
        return "CassandraJMXCompactionManager [compactionClients=" + compactionClients + "]";
    }
}