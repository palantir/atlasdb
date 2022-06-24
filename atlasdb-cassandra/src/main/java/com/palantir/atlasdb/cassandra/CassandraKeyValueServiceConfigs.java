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
package com.palantir.atlasdb.cassandra;

import com.palantir.atlasdb.spi.DerivedSnapshotConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.refreshable.Refreshable;
import com.palantir.util.OptionalResolver;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface CassandraKeyValueServiceConfigs {
    CassandraKeyValueServiceConfig installConfig();

    Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig();

    DerivedSnapshotConfig derivedSnapshotConfig();

    static Optional<CassandraKeyValueServiceConfigs> fromKeyValueServiceConfigs(
            KeyValueServiceConfig install, Refreshable<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig) {
        if (install.type().equals(CassandraKeyValueServiceConfig.TYPE)) {
            CassandraKeyValueServiceConfig cassInstall = (CassandraKeyValueServiceConfig) install;
            Refreshable<KeyValueServiceRuntimeConfig> kvsRuntime = runtimeConfig.map(
                    maybeConfig -> maybeConfig.orElseGet(CassandraKeyValueServiceRuntimeConfig::getDefault));

            Refreshable<CassandraKeyValueServiceRuntimeConfig> cassRuntime = RefreshableWithInitialValue.of(
                    kvsRuntime,
                    CassandraKeyValueServiceConfigs::castOrThrow,
                    CassandraKeyValueServiceRuntimeConfig.getDefault());

            Refreshable<CassandraReloadableKeyValueServiceRuntimeConfig> mergedConfig =
                    CassandraReloadableKeyValueServiceRuntimeConfig.fromConfigs(cassInstall, cassRuntime);

            // Safety: CassandraReloadableKVSRuntimeConfig is a subtype of CassandraKVSRuntimeConfig, but Refreshable
            // isn't covariant in the generic type arg, so the cast is required.
            Refreshable<CassandraKeyValueServiceRuntimeConfig> castedMergedConfig =
                    mergedConfig.map(CassandraKeyValueServiceRuntimeConfig.class::cast);

            DerivedSnapshotConfig derivedSnapshotConfig = DerivedSnapshotConfig.builder()
                    .concurrentGetRangesThreadPoolSize(mergedConfig.get().concurrentGetRangesThreadPoolSize())
                    .defaultGetRangesConcurrencyOverride(mergedConfig.get().defaultGetRangesConcurrency())
                    .build();

            return Optional.of(builder()
                    .installConfig(cassInstall)
                    .runtimeConfig(castedMergedConfig)
                    .derivedSnapshotConfig(derivedSnapshotConfig)
                    .build());
        } else {
            return Optional.empty();
        }
    }

    static CassandraKeyValueServiceConfigs fromKeyValueServiceConfigsOrThrow(
            KeyValueServiceConfig install, Refreshable<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig) {
        return fromKeyValueServiceConfigs(install, runtimeConfig)
                .orElseThrow(() -> new SafeIllegalArgumentException(
                        "Invalid KeyValueServiceConfig. Expected a KeyValueServiceConfig of type"
                                + " CassandraKeyValueServiceConfig, but found a different type.",
                        SafeArg.of("configType", install.type())));
    }

    default CassandraKeyValueServiceConfigs copyWithKeyspace(String recommendedKeyspace) {
        return builder()
                .from(this)
                .installConfig(ImmutableCassandraKeyValueServiceConfig.builder()
                        .from(this.installConfig())
                        .keyspace(recommendedKeyspace)
                        .build())
                .build();
    }

    default CassandraKeyValueServiceConfig copyWithKeyspace(
            CassandraKeyValueServiceConfig config, String recommendedKeyspace) {
        return ImmutableCassandraKeyValueServiceConfig.builder()
                .from(config)
                .keyspace(recommendedKeyspace)
                .build();
    }

    /***
     * 1. If the keyspace on the install config matches a non-empty keyspace provided, then the install config will
     * use that keyspace.
     * 2. If the keyspace on the install config is not present, and a keyspace is provided, then the resultant
     * install config will use the provided keyspace.
     * 3. If the keyspace on the install config is present and a keyspace is not provided, then the resultant install
     * config will use the original install config keyspace.
     *
     * In all other cases, this method will throw a {@link SafeIllegalArgumentException} (due to a missing keyspace or
     * contradictory keyspaces)
     */
    default CassandraKeyValueServiceConfigs copyWithResolvedKeyspace(Optional<String> maybeKeyspace) {
        String desiredKeyspace =
                OptionalResolver.resolve(maybeKeyspace, installConfig().keyspace());
        return this.copyWithKeyspace(desiredKeyspace);
    }

    static ImmutableCassandraKeyValueServiceConfigs.Builder builder() {
        return ImmutableCassandraKeyValueServiceConfigs.builder();
    }

    private static CassandraKeyValueServiceRuntimeConfig castOrThrow(KeyValueServiceRuntimeConfig value) {
        if (!(value instanceof CassandraKeyValueServiceRuntimeConfig)) {
            throw new SafeIllegalArgumentException(
                    "Invalid KeyValueServiceRuntimeConfig. Expected a KeyValueServiceRuntimeConfig of"
                            + " type CassandraKeyValueServiceRuntimeConfig. Using latest valid"
                            + " CassandraKeyValueServiceRuntimeConfig.",
                    SafeArg.of("configClass", value.getClass()));
        }
        return (CassandraKeyValueServiceRuntimeConfig) value;
    }
}
