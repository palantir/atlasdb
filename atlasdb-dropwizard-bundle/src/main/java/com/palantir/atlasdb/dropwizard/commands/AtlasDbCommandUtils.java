/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
 *
 */
package com.palantir.atlasdb.dropwizard.commands;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbConfigs;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ImmutableTimeLockClientConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.config.TimeLockRuntimeConfig;

public final class AtlasDbCommandUtils {
    private static final String TIMELOCK_CLIENT_DOCS_URL
            = "http://palantir.github.io/atlasdb/html/configuration/external_timelock_service_configs/timelock_client_config.html";
    private static final String LEADER_CONFIG_DOCS_URL
            = "http://palantir.github.io/atlasdb/html/configuration/leader_config.html";

    public static final Object ZERO_ARITY_ARG_CONSTANT = "<ZERO ARITY ARG CONSTANT>";
    public static final String OFFLINE_COMMAND_ARG_NAME = "--offline";

    private AtlasDbCommandUtils() {
        // Static utility class
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static AtlasDbConfig convertServerConfigToClientConfig(AtlasDbConfig installConfig,
            Optional<AtlasDbRuntimeConfig> runtimeConfig) {
        Preconditions.checkArgument(installConfig.leader().isPresent()
                        || installConfig.timelock().isPresent()
                        || runtimeConfigHasTimeLockBlock(runtimeConfig),
                "Your server configuration file must have a leader — in the install time config — "
                        + "or timelock block — in either the install or the runtime config. "
                        + "For instructions on how to do "
                        + "this, see the documentation: "
                        + LEADER_CONFIG_DOCS_URL
                        + " or "
                        + TIMELOCK_CLIENT_DOCS_URL
                        + ", respectively.");

        if (installConfig.timelock().isPresent()) {
            return installConfig;
        }

        if (runtimeConfigHasTimeLockBlock(runtimeConfig)) {
            //noinspection ConstantConditions
            return convertRuntimeConfigWithTimeLock(installConfig, runtimeConfig.get());
        }

        return convertConfigWithLeaderBlockToClientConfig(installConfig);
    }

    private static boolean runtimeConfigHasTimeLockBlock(Optional<AtlasDbRuntimeConfig> runtimeConfig) {
        return runtimeConfig.flatMap(AtlasDbRuntimeConfig::timelockRuntime).isPresent();
    }

    private static AtlasDbConfig convertRuntimeConfigWithTimeLock(AtlasDbConfig installConfig,
            AtlasDbRuntimeConfig runtimeConfig) {
        TimeLockRuntimeConfig timeLockRuntimeConfig = runtimeConfig.timelockRuntime().get();
        ImmutableTimeLockClientConfig timeLockClientConfig = ImmutableTimeLockClientConfig.builder()
                .client(installConfig.getNamespaceString())
                .serversList(timeLockRuntimeConfig.serversList())
                .build();

        return ImmutableAtlasDbConfig.builder()
                .from(installConfig)
                .timelock(timeLockClientConfig)
                .build();
    }

    private static AtlasDbConfig convertConfigWithLeaderBlockToClientConfig(AtlasDbConfig serverConfig) {
        ServerListConfig leaders = ImmutableServerListConfig.builder()
                .servers(serverConfig.leader().get().leaders())
                .sslConfiguration(serverConfig.leader().get().sslConfiguration())
                .build();

        return ImmutableAtlasDbConfig.builder()
                .from(serverConfig)
                .leader(Optional.empty())
                .lock(leaders)
                .timestamp(leaders)
                .build();
    }

    public static List<String> gatherPassedInArguments(Map<String, Object> allArgs) {
        return allArgs.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith("--"))
                .filter(entry -> entry.getValue() != null)
                .flatMap(entry -> {
                    if (entry.getValue() instanceof List) {
                        return Stream.concat(Stream.of(entry.getKey()), ((List<String>) entry.getValue()).stream());
                    } else if (entry.getValue().equals(ZERO_ARITY_ARG_CONSTANT)) {
                        return Stream.of(entry.getKey());
                    } else {
                        return Stream.of(entry.getKey(), (String) entry.getValue());
                    }
                })
                .collect(Collectors.toList());
    }

    public static String serialiseConfiguration(AtlasDbConfig cliConfiguration) throws JsonProcessingException {
        return AtlasDbConfigs.OBJECT_MAPPER.writeValueAsString(cliConfiguration);
    }
}
