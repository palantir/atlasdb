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

package com.palantir.atlasdb.cassandra;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableSet;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.immutables.value.Value;

public final class CassandraServersConfigs {
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraServersConfigs.class);

    private CassandraServersConfigs() {}

    private static final String PORT_NUMBER_ERROR = "%s port number should be a positive number";

    private static void checkPortNumbers(Set<InetSocketAddress> socketAddresses, String portName) {
        socketAddresses.forEach(host ->
                Preconditions.checkState(host.getPort() > 0, PORT_NUMBER_ERROR, SafeArg.of(portName, host.getPort())));
    }

    public interface Visitor<T> {
        T visit(DefaultConfig defaultConfig);

        T visit(CqlCapableConfig cqlCapableConfig);
    }

    public enum ThriftHostsExtractingVisitor implements Visitor<ImmutableSet<InetSocketAddress>> {
        INSTANCE;

        @Override
        public ImmutableSet<InetSocketAddress> visit(DefaultConfig defaultConfig) {
            return ImmutableSet.copyOf(defaultConfig.thriftHosts());
        }

        @Override
        public ImmutableSet<InetSocketAddress> visit(CqlCapableConfig cqlCapableConfig) {
            return ImmutableSet.copyOf(cqlCapableConfig.thriftHosts());
        }
    }

    @JsonDeserialize(as = ImmutableCqlCapableConfigTuning.class)
    @JsonSerialize(as = ImmutableCqlCapableConfigTuning.class)
    @Value.Immutable
    public static class CqlCapableConfigTuning {
        @Value.Default
        public int preparedStatementCacheSize() {
            return 100;
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = ImmutableDefaultConfig.class)
    @JsonSubTypes({
        @JsonSubTypes.Type(value = ImmutableDefaultConfig.class, name = DefaultConfig.TYPE),
        @JsonSubTypes.Type(value = ImmutableCqlCapableConfig.class, name = CqlCapableConfig.TYPE)
    })
    public interface CassandraServersConfig {

        <T> T accept(Visitor<T> visitor);

        int numberOfThriftHosts();
    }

    @Value.Immutable(singleton = true)
    @JsonDeserialize(as = ImmutableDefaultConfig.class)
    @JsonSerialize(as = ImmutableDefaultConfig.class)
    @JsonTypeName(DefaultConfig.TYPE)
    public abstract static class DefaultConfig implements CassandraServersConfig {
        static final String TYPE = "default";

        @JsonValue
        public abstract Set<InetSocketAddress> thriftHosts();

        @Override
        @Value.Derived
        public int numberOfThriftHosts() {
            return thriftHosts().size();
        }

        @Value.Check
        final void check() {
            checkPortNumbers(thriftHosts(), "'port'");
        }

        @Override
        public <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    @JsonDeserialize(as = ImmutableCqlCapableConfig.class)
    @JsonSerialize(as = ImmutableCqlCapableConfig.class)
    @JsonTypeName(CqlCapableConfig.TYPE)
    @Value.Immutable
    public abstract static class CqlCapableConfig implements CassandraServersConfig {
        static final String TYPE = "cqlCapable";

        public abstract Set<InetSocketAddress> thriftHosts();

        public abstract Set<InetSocketAddress> cqlHosts();

        @Value.Default
        public CqlCapableConfigTuning tuning() {
            return ImmutableCqlCapableConfigTuning.builder().build();
        }

        @Override
        @Value.Derived
        public int numberOfThriftHosts() {
            return thriftHosts().size();
        }

        @Value.Check
        final void check() {
            checkPortNumbers(thriftHosts(), "'thriftPort'");
            checkPortNumbers(cqlHosts(), "'cqlPort'");
        }

        @SuppressWarnings("ReverseDnsLookup") // May have raw IPs that we want to validate
        public boolean validateHosts() {
            return thriftHosts().stream()
                    .map(InetSocketAddress::getHostName)
                    .collect(Collectors.toSet())
                    .equals(cqlHosts().stream()
                            .map(InetSocketAddress::getHostName)
                            .collect(Collectors.toSet()));
        }

        @Override
        public <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    public static Set<InetSocketAddress> getCqlHosts(CassandraServersConfig config) {
        return CassandraServersConfigs.getCqlCapableConfigIfValid(config)
                .map(CassandraServersConfigs.CqlCapableConfig::cqlHosts)
                .orElseThrow(() -> new SafeIllegalStateException("Attempting to get CQL hosts with thrift config!"));
    }

    public static Optional<CqlCapableConfig> getCqlCapableConfigIfValid(CassandraServersConfig config) {
        return config.accept(new Visitor<>() {
            @Override
            public Optional<CqlCapableConfig> visit(DefaultConfig defaultConfig) {
                return Optional.empty();
            }

            @Override
            public Optional<CqlCapableConfig> visit(CqlCapableConfig cqlCapableConfig) {
                if (!cqlCapableConfig.validateHosts()) {
                    log.warn("Your CQL capable config is wrong, the hosts for CQL and Thrift are not the same.");
                    return Optional.empty();
                }

                return Optional.of(cqlCapableConfig);
            }
        });
    }
}
