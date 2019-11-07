/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.keyvalue.cassandra.async.client.creation;

import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CqlCapableConfig;
import com.palantir.atlasdb.keyvalue.cassandra.async.CqlClient;
import com.palantir.atlasdb.keyvalue.cassandra.async.CqlClientImpl;
import com.palantir.atlasdb.keyvalue.cassandra.async.ThrowingCqlClientImpl;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class DefaultCqlClientFactory implements CqlClientFactory {
    public static final CqlClientFactory DEFAULT = new DefaultCqlClientFactory();

    private static final Logger log = LoggerFactory.getLogger(DefaultCqlClientFactory.class);
    private static final String LOAD_BALANCING_POLICY = "DcInferringLoadBalancingPolicy";
    private static final String COMPRESSION_PROTOCOL = "lz4";

    private final Supplier<CqlSessionBuilder> cqlSessionBuilderFactory;

    public DefaultCqlClientFactory(Supplier<CqlSessionBuilder> cqlSessionBuilderFactory) {
        this.cqlSessionBuilderFactory = cqlSessionBuilderFactory;
    }

    public DefaultCqlClientFactory() {
        this(CqlSessionBuilder::new);
    }

    @Override
    public CqlClient constructClient(
            TaggedMetricRegistry taggedMetricRegistry,
            CassandraKeyValueServiceConfig config,
            boolean initializeAsync) {
        return config.servers().accept(new CassandraServersConfigs.Visitor<CqlClient>() {
            @Override
            public CqlClient visit(CassandraServersConfigs.DefaultConfig defaultConfig) {
                return ThrowingCqlClientImpl.INSTANCE;
            }

            @Override
            public CqlClient visit(CqlCapableConfig cqlCapableConfig) {
                if (!cqlCapableConfig.thriftAndCqlHostsMatch()) {
                    log.warn("Your CQL capable config is wrong, the hosts for CQL and Thrift are not the same, using "
                            + "async API will result in an exception.");
                    return ThrowingCqlClientImpl.INSTANCE;
                }

                Set<InetSocketAddress> servers = cqlCapableConfig.cqlHosts();
                DriverConfigLoader driverConfigLoader = buildDriverConfigLoader(config);
                Supplier<CqlSession> cqlSessionSupplier =
                        constructCqlSessionFactory(servers, driverConfigLoader, config);

                return CqlClientImpl.create(
                        taggedMetricRegistry,
                        cqlSessionSupplier,
                        cqlCapableConfig.tuning(),
                        initializeAsync);
            }
        });
    }

    private DriverConfigLoader buildDriverConfigLoader(CassandraKeyValueServiceConfig config) {
        return DriverConfigLoader.programmaticBuilder()
                .withString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, LOAD_BALANCING_POLICY)
                .withString(DefaultDriverOption.PROTOCOL_COMPRESSION, COMPRESSION_PROTOCOL)
                .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, config.poolSize())
                .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, config.poolSize())
                .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, config.fetchBatchCount())
                .build();
    }

    private Supplier<CqlSession> constructCqlSessionFactory(
            Set<InetSocketAddress> contactPoints,
            DriverConfigLoader driverConfigLoader,
            CassandraKeyValueServiceConfig config) {
        return () -> {
            CqlSessionBuilder cqlSessionBuilder = cqlSessionBuilderFactory.get();
            cqlSessionBuilder.addContactPoints(contactPoints)
                    .withAuthCredentials(config.credentials().username(), config.credentials().password())
                    .withConfigLoader(driverConfigLoader);
            return withSslOptions(cqlSessionBuilder, config).build();
        };
    }

    private static CqlSessionBuilder withSslOptions(CqlSessionBuilder builder, CassandraKeyValueServiceConfig config) {
        if (!config.usingSsl()) {
            return builder;
        }
        if (config.sslConfiguration().isPresent()) {
            return builder.withSslContext(SslSocketFactories.createSslContext(config.sslConfiguration().get()));
        }
        try {
            return builder.withSslContext(SSLContext.getDefault());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e.getCause());
        }
    }
}
