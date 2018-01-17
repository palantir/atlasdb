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

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.management.remote.JMXConnector;

import org.apache.cassandra.db.HintedHandOffManagerMBean;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.service.StorageServiceMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.cassandra.CassandraJmxCompactionConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraConstants;
import com.palantir.common.concurrent.PTExecutors;

public final class CassandraJmxCompaction {
    private static final Logger log = LoggerFactory.getLogger(CassandraJmxCompaction.class);
    private final CassandraKeyValueServiceConfig config;

    private CassandraJmxCompaction(CassandraKeyValueServiceConfig config) {
        this.config = Preconditions.checkNotNull(config, "config cannot be null");
    }

    public static Optional<CassandraJmxCompactionManager> createJmxCompactionManager(
            CassandraKeyValueServiceConfigManager configManager) {
        Preconditions.checkNotNull(configManager, "configManager cannot be null");
        CassandraKeyValueServiceConfig config = configManager.getConfig();
        CassandraJmxCompaction jmxCompaction = new CassandraJmxCompaction(config);

        Optional<CassandraJmxCompactionConfig> jmxConfig = config.jmx();
        // need to set the property before creating the JMX compaction client
        if (!jmxConfig.isPresent()) {
            log.info("Jmx compaction is not enabled.");
            return Optional.empty();
        }

        jmxCompaction.setJmxSslProperty(jmxConfig.get());
        ImmutableSet<CassandraJmxCompactionClient> clients = jmxCompaction.createCompactionClients(jmxConfig.get());
        ExecutorService exec = PTExecutors.newFixedThreadPool(clients.size(),
                new ThreadFactoryBuilder().setNameFormat("Cassandra-Jmx-Compaction-ThreadPool-%d").build());

        return Optional.of(CassandraJmxCompactionManager.create(clients, exec));
    }

    /**
     * Running compaction against C* without SSL enabled experiences some hanging tcp connection
     * during compaction processes. "sun.rmi.transport.tcp.responseTimeout" will enforce the tcp
     * connection timeout in case it happens.
     */
    private void setJmxSslProperty(CassandraJmxCompactionConfig jmxConfig) {
        long rmiTimeoutMillis = jmxConfig.rmiTimeoutMillis();
        // NOTE: RMI timeout to avoid hanging tcp connection
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", String.valueOf(rmiTimeoutMillis));

        if (!jmxConfig.ssl()) {
            return;
        }

        String keyStoreFile = jmxConfig.keystore();
        String keyStorePassword = jmxConfig.keystorePassword();
        String trustStoreFile = jmxConfig.truststore();
        String trustStorePassword = jmxConfig.truststorePassword();
        Preconditions.checkState((new File(keyStoreFile)).exists(), "file: '%s' does not exist!", keyStoreFile);
        Preconditions.checkState((new File(trustStoreFile)).exists(), "file: '%s' does not exist!", trustStoreFile);
        System.setProperty("javax.net.ssl.keyStore", keyStoreFile);
        System.setProperty("javax.net.ssl.keyStorePassword", keyStorePassword);
        System.setProperty("javax.net.ssl.trustStore", trustStoreFile);
        System.setProperty("javax.net.ssl.trustStorePassword", trustStorePassword);
    }

    /**
     * Return an empty set if no client can be created.
     */
    private ImmutableSet<CassandraJmxCompactionClient> createCompactionClients(CassandraJmxCompactionConfig jmxConfig) {
        Set<CassandraJmxCompactionClient> clients = Sets.newHashSet();
        Set<InetSocketAddress> servers = config.servers();
        int jmxPort = jmxConfig.port();
        for (InetSocketAddress addr : servers) {
            CassandraJmxCompactionClient client =
                    createCompactionClient(addr.getHostString(), jmxPort, jmxConfig.username(), jmxConfig.password());
            clients.add(client);
        }

        return ImmutableSet.copyOf(clients);
    }

    private CassandraJmxCompactionClient createCompactionClient(
            String host,
            int port,
            String username,
            String password) {
        CassandraJmxConnectorFactory jmxConnectorFactory =
                new CassandraJmxConnectorFactory(host, port, username, password);
        JMXConnector jmxConnector = jmxConnectorFactory.create();

        CassandraJmxBeanFactory jmxBeanFactory = new CassandraJmxBeanFactory(jmxConnector);
        StorageServiceMBean storageServiceProxy = jmxBeanFactory.create(
                CassandraConstants.STORAGE_SERVICE_OBJECT_NAME,
                StorageServiceMBean.class);
        HintedHandOffManagerMBean hintedHandoffProxy = jmxBeanFactory.create(
                CassandraConstants.HINTED_HANDOFF_MANAGER_OBJECT_NAME,
                HintedHandOffManagerMBean.class);
        CompactionManagerMBean compactionManagerProxy = jmxBeanFactory.create(
                CassandraConstants.COMPACTION_MANAGER_OBJECT_NAME,
                CompactionManagerMBean.class);

        return CassandraJmxCompactionClient.create(
                host,
                port,
                jmxConnector,
                storageServiceProxy,
                hintedHandoffProxy,
                compactionManagerProxy);
    }
}
