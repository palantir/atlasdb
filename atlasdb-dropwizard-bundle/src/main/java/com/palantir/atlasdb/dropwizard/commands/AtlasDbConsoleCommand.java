package com.palantir.atlasdb.dropwizard.commands;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Optional;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.console.AtlasConsoleMain;
import com.palantir.atlasdb.dropwizard.AtlasDbConfigurationProvider;

import io.dropwizard.Configuration;
import io.dropwizard.jackson.DiscoverableSubtypeResolver;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;

public class AtlasDbConsoleCommand<T extends Configuration & AtlasDbConfigurationProvider> extends AtlasDbCommand<T> {
    private static final ObjectMapper OBJECT_MAPPER;

    static {
        YAMLFactory yamlFactory = new YAMLFactory();
        yamlFactory.configure(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID, false);
        OBJECT_MAPPER = new ObjectMapper(yamlFactory);
        OBJECT_MAPPER.registerModule(new GuavaModule());
    }

    public AtlasDbConsoleCommand(Class<T> configurationClass) {
        super("console", "Open an AtlasDB console", configurationClass);
    }

    @Override
    protected void run(Bootstrap<T> bootstrap, Namespace namespace, T configuration) throws Exception {
        AtlasDbConfig configurationWithoutLeader = ImmutableAtlasDbConfig.builder()
                .from(configuration.getAtlasConfig())
                .leader(Optional.absent())
                .build();

        AtlasConsoleMain.main(new String[] {
                "-b", "dropwizardAtlasDb", OBJECT_MAPPER.writeValueAsString(configurationWithoutLeader),
                "-e", "connectInline dropwizardAtlasDb"
        });
    }
}
