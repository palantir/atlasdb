/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.dbkvs;

import static com.palantir.logsafe.Preconditions.checkArgument;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.service.AutoService;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.LocalConnectionConfig;
import com.palantir.atlasdb.spi.SharedResourcesConfig;
import com.palantir.logsafe.SafeArg;
import com.palantir.nexus.db.pool.config.ConnectionConfig;
import java.util.Optional;
import org.immutables.value.Value;

@AutoService(KeyValueServiceConfig.class)
@JsonDeserialize(as = ImmutableDbKeyValueServiceConfig.class)
@JsonSerialize(as = ImmutableDbKeyValueServiceConfig.class)
@JsonTypeName(DbAtlasDbFactory.TYPE)
@Value.Immutable
public abstract class DbKeyValueServiceConfig implements KeyValueServiceConfig {
    public abstract DdlConfig ddl();

    public abstract ConnectionConfig connection();

    @Override
    @JsonIgnore
    @Value.Derived
    public Optional<String> namespace() {
        return namespaceOverride().or(() -> connection().namespace());
    }

    @JsonProperty("namespace")
    public abstract Optional<String> namespaceOverride();

    @Override
    public final String type() {
        return DbAtlasDbFactory.TYPE;
    }

    @Value.Default
    public int concurrentGetRangesThreadPoolSize() {
        int poolSize = sharedResourcesConfig()
                .map(SharedResourcesConfig::connectionConfig)
                .map(LocalConnectionConfig::poolSize)
                .orElseGet(() -> connection().getMaxConnections());
        return Math.max(2 * poolSize / 3, 1);
    }

    abstract Optional<Integer> defaultGetRangesConcurrency();

    @Override
    @Value.Default
    public boolean enableNamespaceDeletionDangerousIKnowWhatIAmDoing() {
        return false;
    }

    @Value.Check
    public void checkKvsPoolSize() {
        sharedResourcesConfig()
                .ifPresent(config -> checkArgument(
                        config.sharedKvsExecutorSize() >= ddl().poolSize(),
                        "If set, shared kvs pool size must not be less than individual pool size.",
                        SafeArg.of("shared", config.sharedKvsExecutorSize()),
                        SafeArg.of("individual", ddl().poolSize())));
    }

    @Value.Check
    public void checkGetRangesPoolSize() {
        sharedResourcesConfig()
                .ifPresent(config -> checkArgument(
                        config.sharedGetRangesPoolSize() >= concurrentGetRangesThreadPoolSize(),
                        "If set, shared get ranges pool size must not be less than individual pool size.",
                        SafeArg.of("shared", config.sharedGetRangesPoolSize()),
                        SafeArg.of("individual", concurrentGetRangesThreadPoolSize())));
    }

    @Value.Check
    public void checkConnectionPoolSize() {
        sharedResourcesConfig()
                .ifPresent(config -> checkArgument(
                        config.connectionConfig().poolSize() <= connection().getMaxConnections(),
                        "If set, local connection pool size must not be greater than total max connections.",
                        SafeArg.of("local", config.connectionConfig().poolSize()),
                        SafeArg.of("total", connection().getMaxConnections())));
    }

    @Value.Check
    protected final void check() {
        checkArgument(
                ddl().type().equals(connection().type()),
                "ddl config and connection config must be for the same physical store",
                SafeArg.of("ddl", ddl().type()),
                SafeArg.of("connection", connection().type()));
    }
}
