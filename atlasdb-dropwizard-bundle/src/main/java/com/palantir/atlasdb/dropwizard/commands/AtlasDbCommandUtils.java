/**
 * Copyright 2016 Palantir Technologies
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbConfigs;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ServerListConfig;

public final class AtlasDbCommandUtils {
    public static final Object ZERO_ARITY_ARG_CONSTANT = "<ZERO ARITY ARG CONSTANT>";

    private AtlasDbCommandUtils() {
        // Static utility class
    }

    public static AtlasDbConfig convertServerConfigToClientConfig(AtlasDbConfig serverConfig) {
        Preconditions.checkArgument(serverConfig.leader().isPresent(),
                "Only server configurations with a leader block can be converted to client configurations");

        ServerListConfig leaders = ImmutableServerListConfig.builder()
                .servers(serverConfig.leader().get().leaders())
                .sslConfiguration(serverConfig.leader().get().sslConfiguration())
                .build();

        return ImmutableAtlasDbConfig.builder()
                .from(serverConfig)
                .leader(Optional.absent())
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
