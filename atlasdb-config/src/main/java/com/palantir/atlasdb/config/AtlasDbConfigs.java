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
package com.palantir.atlasdb.config;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.base.Strings;
import com.palantir.config.crypto.DecryptingVariableSubstitutor;
import com.palantir.config.crypto.jackson.JsonNodeStringReplacer;
import com.palantir.config.crypto.jackson.JsonNodeVisitors;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nullable;

public final class AtlasDbConfigs {
    public static final String ATLASDB_CONFIG_OBJECT_PATH = "/atlasdb";

    static final String DISCOVERED_SUBTYPE_MARKER = "io.dropwizard.jackson.Discoverable";

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory()
            .disable(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID)
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER))
            .setSubtypeResolver(new DiscoverableSubtypeResolver(DISCOVERED_SUBTYPE_MARKER))
            .registerModule(new GuavaModule())
            .registerModule(new Jdk8Module());

    private AtlasDbConfigs() {
        // uninstantiable
    }

    public static <T> T load(File configFile, Class<T> clazz) throws IOException {
        return load(configFile, ATLASDB_CONFIG_OBJECT_PATH, clazz);
    }

    public static <T> T load(InputStream configStream, Class<T> clazz) throws IOException {
        return loadFromStream(configStream, ATLASDB_CONFIG_OBJECT_PATH, clazz);
    }

    public static <T> T load(File configFile, @Nullable String configRoot, Class<T> clazz) throws IOException {
        JsonNode node = OBJECT_MAPPER.readTree(configFile);
        return getConfig(node, configRoot, clazz);
    }

    public static <T> T loadFromString(String fileContents, @Nullable String configRoot, Class<T> clazz)
            throws IOException {
        JsonNode node = OBJECT_MAPPER.readTree(fileContents);
        return getConfig(node, configRoot, clazz);
    }

    public static <T> T loadFromStream(InputStream configStream, @Nullable String configRoot, Class<T> clazz)
            throws IOException {
        JsonNode node = OBJECT_MAPPER.readTree(configStream);
        return getConfig(node, configRoot, clazz);
    }

    private static <T> T getConfig(JsonNode node, @Nullable String configRoot, Class<T> clazz) throws IOException {
        JsonNode configNode = findRoot(node, configRoot);

        if (configNode == null) {
            throw new IllegalArgumentException("Could not find " + configRoot + " in input");
        }

        return OBJECT_MAPPER.treeToValue(decryptConfigValues(configNode), clazz);
    }

    private static JsonNode decryptConfigValues(JsonNode configNode) {
        return JsonNodeVisitors.dispatch(
                OBJECT_MAPPER.valueToTree(configNode),
                new JsonNodeStringReplacer(new DecryptingVariableSubstitutor()));
    }

    private static JsonNode findRoot(JsonNode node, @Nullable String configRoot) {
        if (Strings.isNullOrEmpty(configRoot)) {
            return node;
        }

        JsonNode root = node.at(JsonPointer.valueOf(configRoot));
        if (root.isMissingNode()) {
            return null;
        }
        return root;
    }

    public static AtlasDbConfig addFallbackSslConfigurationToAtlasDbConfig(
            AtlasDbConfig config,
            Optional<SslConfiguration> sslConfiguration) {
        return ImmutableAtlasDbConfig.builder()
                .from(config)
                .leader(addFallbackSslConfigurationToLeader(config.leader(), sslConfiguration))
                .lock(addFallbackSslConfigurationToServerList(config.lock(), sslConfiguration))
                .timestamp(addFallbackSslConfigurationToServerList(config.timestamp(), sslConfiguration))
                .timelock(addFallbackSslConfigurationToTimeLockClientConfig(config.timelock(), sslConfiguration))
                .build();
    }

    private static Optional<LeaderConfig> addFallbackSslConfigurationToLeader(
            Optional<LeaderConfig> config,
            Optional<SslConfiguration> sslConfiguration) {
        return config.map(leader -> ImmutableLeaderConfig.builder()
                .from(leader)
                .sslConfiguration(getFirstPresentOptional(leader.sslConfiguration(), sslConfiguration))
                .build());
    }

    private static Optional<ServerListConfig> addFallbackSslConfigurationToServerList(
            Optional<ServerListConfig> config,
            Optional<SslConfiguration> sslConfiguration) {
        return config.map(addSslConfigurationToServerListFunction(sslConfiguration));
    }

    private static Optional<TimeLockClientConfig> addFallbackSslConfigurationToTimeLockClientConfig(
            Optional<TimeLockClientConfig> config,
            Optional<SslConfiguration> sslConfiguration) {
        //noinspection ConstantConditions - function returns an existing ServerListConfig, maybe with different SSL.
        return config.map(clientConfig -> ImmutableTimeLockClientConfig.builder()
                .from(clientConfig)
                .serversList(addSslConfigurationToServerListFunction(sslConfiguration)
                        .apply(clientConfig.serversList()))
                .build());
    }

    private static Function<ServerListConfig, ServerListConfig> addSslConfigurationToServerListFunction(
            Optional<SslConfiguration> sslConfiguration) {
        return serverList -> ImmutableServerListConfig.builder()
                .from(serverList)
                .sslConfiguration(getFirstPresentOptional(serverList.sslConfiguration(), sslConfiguration))
                .build();
    }

    // this behavior mimics guava's "public abstract Optional<T> or(Optional<? extends T> secondChoice)" method
    private static <T> Optional<T> getFirstPresentOptional(Optional<T> primary, Optional<T> secondary) {
        return primary.isPresent() ? primary : secondary;
    }
}
