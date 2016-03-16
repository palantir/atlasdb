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

    public static AtlasDbServicesModule create(File configFile, String configRoot) throws IOException {
        ObjectMapper configMapper = Jackson.newObjectMapper(new YAMLFactory());
        JsonNode node = getConfigNode(configMapper, configFile, configRoot);
        AtlasDbServerConfiguration config = configMapper.treeToValue(node, AtlasDbServerConfiguration.class);
        return new AtlasDbServicesModule(config.getConfig());
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
