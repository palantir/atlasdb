package com.palantir.atlasdb.keyvalue.cassandra;

import java.net.InetSocketAddress;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;

/**
 * Contains KVS instance for the Cassandra Testsuite
 *
 * Created by aloro on 12/04/2016.
 */
class CassandraTestConfigs {
    static final String CASSANDRA_HOST = "cassandra";

    static final int THRIFT_PORT = 9160;
    static final int CQL_PORT = 9042;

    static final CassandraKeyValueServiceConfig thriftConfigurationSafetyEnabled = ImmutableCassandraKeyValueServiceConfig.builder()
            .addServers(new InetSocketAddress(CASSANDRA_HOST, THRIFT_PORT))
            .poolSize(20)
            .keyspace("atlasdb")
            .ssl(false)
            .replicationFactor(1)
            .mutationBatchCount(10000)
            .mutationBatchSizeBytes(10000000)
            .fetchBatchCount(1000)
            .safetyDisabled(true)
            .autoRefreshNodes(false)
            .build();
    static final CassandraKeyValueServiceConfig thriftConfigurationSafetyDisabled = ImmutableCassandraKeyValueServiceConfig.builder()
            .addServers(new InetSocketAddress(CASSANDRA_HOST, THRIFT_PORT))
            .poolSize(20)
            .keyspace("atlasdb")
            .ssl(false)
            .replicationFactor(1)
            .mutationBatchCount(10000)
            .mutationBatchSizeBytes(10000000)
            .fetchBatchCount(1000)
            .safetyDisabled(false)
            .autoRefreshNodes(false)
            .build();
    static final CassandraKeyValueServiceConfig cqlConfiguration = ImmutableCassandraKeyValueServiceConfig.builder()
            .addServers(new InetSocketAddress(CASSANDRA_HOST, CQL_PORT))
            .poolSize(20)
            .keyspace("atlasdb")
            .ssl(false)
            .replicationFactor(1)
            .mutationBatchCount(10000)
            .mutationBatchSizeBytes(10000000)
            .fetchBatchCount(1000)
            .safetyDisabled(true)
            .autoRefreshNodes(false)
            .build();
}
