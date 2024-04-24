package com.palantir.atlasdb.workload.migration.cql;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.palantir.cassandra.manager.core.cql.ImmutableKeyspaceQuery;
import com.palantir.cassandra.manager.core.cql.KeyspaceQuery;
import com.palantir.cassandra.manager.core.cql.KeyspaceQueryMethod;
import com.palantir.cassandra.manager.core.cql.ReplicationOptions;
import com.palantir.cassandra.manager.core.cql.SchemaMutationResult;
import com.palantir.cassandra.manager.objects.SafeKeyspace;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import one.util.streamex.StreamEx;

public class CqlCassandraKeyspaceReplicationStrategyManager implements CassandraKeyspaceReplicationStrategyManager {
    private static final SafeLogger log = SafeLoggerFactory.get(CqlCassandraKeyspaceReplicationStrategyManager.class);
    private static final String TOPOLOGY_STRATEGY_KEY = "class";
    private static final String NETWORK_TOPOLOGY_STRATEGY = "NetworkTopologyStrategy";

    private final Supplier<Session> sessionProvider;

    public CqlCassandraKeyspaceReplicationStrategyManager(Supplier<Session> sessionProvider) {
        this.sessionProvider = sessionProvider;
    }

    @Override
    public SchemaMutationResult setReplicationFactorToThreeForDatacenters(Set<String> datacenters, String keyspace) {
        logAllKeyspaces();
        Map<String, String> datacenterReplicationFactor = StreamEx.of(datacenters)
                .mapToEntry(_datacenter -> "3")
                .append(TOPOLOGY_STRATEGY_KEY, NETWORK_TOPOLOGY_STRATEGY)
                .toMap();
        KeyspaceQuery query = ImmutableKeyspaceQuery.builder()
                .keyspace(SafeKeyspace.of(keyspace))
                .durableWrites(true)
                .replication(ReplicationOptions.of(datacenterReplicationFactor))
                .method(KeyspaceQueryMethod.ALTER)
                .build();
        return runWithCqlSession(query::applyTo);
    }

    private void logAllKeyspaces() {
        List<KeyspaceMetadata> ks =
                runWithCqlSession(session -> session.getCluster().getMetadata().getKeyspaces());
        log.info(
                "Keyspaces {}",
                SafeArg.of("results", ks.stream().map(KeyspaceMetadata::getName).collect(Collectors.toList())));
    }

    private <T> T runWithCqlSession(Function<Session, T> sessionConsumer) {
        try (Session session = sessionProvider.get()) {
            return sessionConsumer.apply(session);
        }
    }
}
