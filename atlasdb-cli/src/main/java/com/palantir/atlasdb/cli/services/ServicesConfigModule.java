/**
 * Copyright 2015 Palantir Technologies
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
 */
package com.palantir.atlasdb.cli.services;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import javax.inject.Singleton;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Strings;
import com.palantir.atlasdb.config.AtlasDbConfig;

import dagger.Module;
import dagger.Provides;
import io.dropwizard.jackson.DiscoverableSubtypeResolver;

@Module
public class ServicesConfigModule {

    private final ServicesConfig config;

    public static ServicesConfigModule create(File configFile, String configRoot) throws IOException {
        ObjectMapper configMapper = new ObjectMapper(new YAMLFactory());
        configMapper.setSubtypeResolver(new DiscoverableSubtypeResolver());
        JsonNode node = getConfigNode(configMapper, configFile, configRoot);
        AtlasDbConfig config = configMapper.treeToValue(node, AtlasDbConfig.class);
        return ServicesConfigModule.create(config);
    }

    private static JsonNode getConfigNode(ObjectMapper configMapper, File configFile, String configRoot) throws IOException {
        JsonNode node = configMapper.readTree(configFile);
        if (Strings.isNullOrEmpty(configRoot)) {
            return node;
        } else {
            JsonNode rootNode = findRoot(node, configRoot);
            if (rootNode != null) {
                return rootNode;
            }
            throw new IllegalArgumentException("Could not find " + configRoot + " in yaml file " + configFile);
        }
    }

    private static JsonNode findRoot(JsonNode node, String configRoot) {
        if (node.has(configRoot)) {
            return node.get(configRoot);
        } else {
            Iterator<String> iter = node.fieldNames();
            while (iter.hasNext()) {
                JsonNode root = findRoot(node.get(iter.next()), configRoot);
                if (root != null) {
                    return root;
                }
            }
            return null;
        }
    }

    public static ServicesConfigModule create(AtlasDbConfig atlasDbConfig) {
        return new ServicesConfigModule(ImmutableServicesConfig.builder().atlasDbConfig(atlasDbConfig).build());
    }

    public ServicesConfigModule(ServicesConfig config) {
        this.config = config;
    }

    @Provides
    @Singleton
    public ServicesConfig provideServicesConfig() { return config; }

    @Provides
    @Singleton
    public AtlasDbConfig provideAtlasDbConfig() { return config.atlasDbConfig(); }

}
