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
 */
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
