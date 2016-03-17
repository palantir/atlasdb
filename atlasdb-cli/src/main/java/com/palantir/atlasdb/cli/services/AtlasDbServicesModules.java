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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Strings;
import com.palantir.atlasdb.server.AtlasDbServerConfiguration;

import dagger.Module;
import io.dropwizard.jackson.Jackson;

@Module
public final class AtlasDbServicesModules {

    private AtlasDbServicesModules() { }

    public static ServicesConfigModule create(File configFile, String configRoot) throws IOException {
        return create(config -> new ServicesConfigModule(config), configFile, configRoot);
    }

    public static ServicesConfigModule create(AtlasDbServicesModuleFactory factory,
                                               File configFile,
                                               String configRoot) throws IOException {
        ObjectMapper configMapper = Jackson.newObjectMapper(new YAMLFactory());
        JsonNode node = getConfigNode(configMapper, configFile, configRoot);
        AtlasDbServerConfiguration config = configMapper.treeToValue(node, AtlasDbServerConfiguration.class);
        return factory.createModule(config.getConfig());
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
}
