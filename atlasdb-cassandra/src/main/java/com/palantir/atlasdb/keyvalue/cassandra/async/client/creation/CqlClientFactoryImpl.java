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
import java.net.SocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CqlCapableConfig;
import com.palantir.atlasdb.keyvalue.cassandra.async.CqlClient;
import com.palantir.atlasdb.keyvalue.cassandra.async.CqlClientImpl;
import com.palantir.atlasdb.keyvalue.cassandra.async.ThrowingCqlClientImpl;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

import io.netty.channel.Channel;
import io.netty.handler.proxy.Socks5ProxyHandler;

public final class CqlClientFactoryImpl implements CqlClientFactory {

    public static final CqlClientFactory DEFAULT = new CqlClientFactoryImpl();
    private static final Logger log = LoggerFactory.getLogger(CqlClientFactoryImpl.class);
    private static final String LOAD_BALANCING_POLICY = "DcInferringLoadBalancingPolicy";
    private static final String COMPRESSION_PROTOCOL = "lz4";

    private CqlClientFactoryImpl() {
        // Use instance
    }

    @Override
    public CqlClient constructClient(
            TaggedMetricRegistry taggedMetricRegistry,
            CassandraKeyValueServiceConfig config,
            boolean initializeAsync) {
        return config.servers().accept(new CassandraServersConfigs.Visitor<CqlClient>() {
            @Override
            public CqlClient visit(CassandraServersConfigs.DefaultConfig defaultConfig) {
                return ThrowingCqlClientImpl.SINGLETON;
            }

            @Override
            public CqlClient visit(CqlCapableConfig cqlCapableConfig) {
                if (!cqlCapableConfig.validateHosts()) {
                    log.warn("Your CQL capable config is wrong, the hosts for CQL and Thrift are not the same, using "
                            + "async API will result in an exception.");
                    return ThrowingCqlClientImpl.SINGLETON;
                }
                Set<InetSocketAddress> servers = cqlCapableConfig.cqlHosts();

                DriverConfigLoader loader =
                        DriverConfigLoader.programmaticBuilder()
                                .withString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, LOAD_BALANCING_POLICY)
                                .withString(DefaultDriverOption.PROTOCOL_COMPRESSION, COMPRESSION_PROTOCOL)
                                .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, config.poolSize())
                                .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, config.poolSize())
                                .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, config.fetchBatchCount())
                                .build();

                Supplier<CqlSession> cqlSessionSupplier = () -> {
                    CqlSessionBuilder cqlSessionBuilder = withSocksProxy(cqlCapableConfig);
                    cqlSessionBuilder.addContactPoints(servers)
                            .withAuthCredentials(config.credentials().username(), config.credentials().password())
                            .withConfigLoader(loader);
                    return withSslOptions(cqlSessionBuilder, config).build();
                };

                return CqlClientImpl.create(
                        taggedMetricRegistry,
                        cqlSessionSupplier,
                        cqlCapableConfig.tuning(),
                        initializeAsync);
            }
        });
    }

    private static CqlSessionBuilder withSocksProxy(CqlCapableConfig cqlCapableConfig) {
        return cqlCapableConfig.socksProxy().map(CustomCqlSessionBuilder::create).orElseGet(CqlSession::builder);
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

    private static final class CustomCqlSessionBuilder extends CqlSessionBuilder {
        private final SocketAddress proxyAddress;

        static CqlSessionBuilder create(SocketAddress proxyAddress) {
            return new CustomCqlSessionBuilder(proxyAddress);
        }

        CustomCqlSessionBuilder(SocketAddress proxyAddress) {
            this.proxyAddress = proxyAddress;
        }

        @Override
        protected DriverContext buildContext(
                DriverConfigLoader configLoader,
                ProgrammaticArguments programmaticArguments) {
            return new WrappingDriverContext(configLoader, programmaticArguments, proxyAddress);
        }
    }

    private static final class SocksProxyNettyOptions implements DelegatingNettyOptions {
        private final SocketAddress proxyAddress;
        private final NettyOptions delegate;

        SocksProxyNettyOptions(NettyOptions nettyOptions, SocketAddress proxyAddress) {
            this.delegate = nettyOptions;
            this.proxyAddress = proxyAddress;
        }

        @Override
        public NettyOptions delegate() {
            return delegate;
        }

        @Override
        public void afterChannelInitialized(Channel channel) {
            delegate.afterChannelInitialized(channel);
            channel.pipeline().addFirst(new Socks5ProxyHandler(proxyAddress));
        }
    }

    private static final class WrappingDriverContext extends DefaultDriverContext {
        private final SocksProxyNettyOptions nettyOptions;

        WrappingDriverContext(
                DriverConfigLoader configLoader,
                ProgrammaticArguments programmaticArguments,
                SocketAddress proxyAddress) {
            super(configLoader, programmaticArguments);
            this.nettyOptions = new SocksProxyNettyOptions(super.getNettyOptions(), proxyAddress);
        }

        @Nonnull
        @Override
        public NettyOptions getNettyOptions() {
            return nettyOptions;
        }
    }
}
