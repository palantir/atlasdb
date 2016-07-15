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
package com.palantir.atlasdb.dropwizard;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.dropwizard.commands.AtlasDbCommand;
import com.palantir.atlasdb.dropwizard.commands.AtlasDbConsoleCommand;
import com.palantir.atlasdb.dropwizard.commands.AtlasDbTimestampCommand;

import io.dropwizard.Configuration;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

public class AtlasDbConfiguredCommand<T extends Configuration & AtlasDbConfigurationProvider>
        extends ConfiguredCommand<T> {
    private static final String COMMAND_NAME_ATTR = "subCommand";

    private final Class<T> configurationClass;
    private final Map<String, AtlasDbCommand<T>> subCommands;

    protected AtlasDbConfiguredCommand(Class<T> configurationClass) {
        super("atlasdb", "Run AtlasDB tasks");

        this.configurationClass = configurationClass;
        this.subCommands = ImmutableMap.<String, AtlasDbCommand<T>>builder()
                .put("console", new AtlasDbConsoleCommand<>(configurationClass))
                .put("timestamp", new AtlasDbTimestampCommand<>(configurationClass))
                .build();
    }

    @Override
    protected Class<T> getConfigurationClass() {
        return configurationClass;
    }

    @Override
    public void configure(Subparser subparser) {
        for (AtlasDbCommand<T> subCommand : subCommands.values()) {
            Subparser parser = subparser.addSubparsers()
                    .addParser(subCommand.getName())
                    .setDefault(COMMAND_NAME_ATTR, subCommand.getName())
                    .description(subCommand.getDescription());
            subCommand.configure(parser);
        }
    }

    @Override
    protected void run(Bootstrap<T> bootstrap, Namespace namespace, T configuration) throws Exception {
        subCommands.get(namespace.getString(COMMAND_NAME_ATTR)).run(bootstrap, namespace);
    }
}
