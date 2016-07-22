package com.palantir.atlasdb.config;

import java.io.File;
import java.io.IOException;

import javax.annotation.Nullable;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Strings;

import io.dropwizard.jackson.DiscoverableSubtypeResolver;

class ConfigFinder {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

    static {
        OBJECT_MAPPER.setSubtypeResolver(new DiscoverableSubtypeResolver());
        OBJECT_MAPPER.registerModule(new GuavaModule());
    }

    private final String configRoot;

    /**
     * @param configRoot - identifier of AtlasDB confs location in the config file, if null the config root will be used
     */
    ConfigFinder(@Nullable String configRoot) {
        this.configRoot = configRoot;
    }

    AtlasDbConfig getConfig(File configFile) throws IOException {
        JsonNode node = OBJECT_MAPPER.readTree(configFile);
        JsonNode configNode = findRoot(node);

        if (configNode == null) {
            throw new IllegalArgumentException("Could not find " + configRoot + " in yaml file " + configFile);
        }

        return OBJECT_MAPPER.treeToValue(configNode, AtlasDbConfig.class);
    }

    AtlasDbConfig getConfig(String fileContents) throws IOException {
        JsonNode node = OBJECT_MAPPER.readTree(fileContents);
        JsonNode configNode = findRoot(node);

        if (configNode == null) {
            throw new IllegalArgumentException("Could not find " + configRoot + " in given string");
        }

        return OBJECT_MAPPER.treeToValue(configNode, AtlasDbConfig.class);
    }

    private JsonNode findRoot(JsonNode node) {
        if (Strings.isNullOrEmpty(configRoot)) {
            return node;
        }

        JsonNode root = node.at(JsonPointer.valueOf(configRoot));
        if (root.isMissingNode()) {
            return null;
        }
        return root;
    }
}
