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
import com.palantir.atlasdb.server.AtlasDbServerConfiguration;

import dagger.Module;
import dagger.Provides;
import io.dropwizard.jackson.Jackson;

@Module
public class ServicesConfigModule {

    private final ServicesConfig config;

    public static ServicesConfigModule create(File configFile, String configRoot) throws IOException {
        ObjectMapper configMapper = Jackson.newObjectMapper(new YAMLFactory());
        JsonNode node = getConfigNode(configMapper, configFile, configRoot);
        AtlasDbServerConfiguration config = configMapper.treeToValue(node, AtlasDbServerConfiguration.class);
        return ServicesConfigModule.create(config.getConfig());
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
    public ServicesConfig provideAtlasDbConfig() { return config; }

}
