package com.palantir.atlasdb.keyvalue.cassandra.jmx;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;

public class CassandraJmxCompactionModule {
    private static final Logger log = LoggerFactory.getLogger(CassandraJmxCompactionModule.class);

    public Optional<CassandraJmxCompactionManager> createCompactionManager(CassandraKeyValueServiceConfigManager configManager) {
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        Set<CassandraJmxCompactionClient> clients = CassandraJmxCompactionManagers.createCompactionClients(config);

        if (clients.isEmpty()) {
            log.warn("No compaction client can be found.");
            return Optional.absent();
        }

        ExecutorService exec = Executors.newFixedThreadPool(
                clients.size(),
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Cassandra-Jmx-Compaction-ThreadPool-%d").build());
        return Optional.of(CassandraJmxCompactionManager.newInstance(clients, exec));
    }
}
