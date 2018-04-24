/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.atlasdb.dropwizard.commands.AtlasDbCliCommand;
import com.palantir.atlasdb.dropwizard.commands.AtlasDbCommand;
import com.palantir.atlasdb.dropwizard.commands.AtlasDbConsoleCommand;

import io.dropwizard.Configuration;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

public class AtlasDbConfiguredCommand<T extends Configuration & AtlasDbConfigurationProvider>
        extends ConfiguredCommand<T> {
    private static final String COMMAND_NAME_ATTR = "subCommand";

    private final Class<T> configurationClass;

    private final AtlasDbCommand<T> consoleCommand;
    private final AtlasDbCommand<T> cliCommand;

    protected AtlasDbConfiguredCommand(Class<T> configurationClass) {
        super("atlasdb", "Run AtlasDB tasks");

        this.configurationClass = configurationClass;
        this.consoleCommand = new AtlasDbConsoleCommand<>(configurationClass);
        this.cliCommand = new AtlasDbCliCommand<>(configurationClass);
    }

    @Override
    protected Class<T> getConfigurationClass() {
        return configurationClass;
    }

    @Override
    public void configure(Subparser subparser) {
        Subparser parser = subparser.addSubparsers()
                .addParser(consoleCommand.getName())
                .setDefault(COMMAND_NAME_ATTR, consoleCommand.getName())
                .description(consoleCommand.getDescription());
        consoleCommand.configure(parser);

        cliCommand.configure(subparser);
    }

    @Override
    protected void run(Bootstrap<T> bootstrap, Namespace namespace, T configuration) throws Exception {
        if (consoleCommand.getName().equals(namespace.getString(COMMAND_NAME_ATTR))) {
            consoleCommand.run(bootstrap, namespace);
        } else {
            cliCommand.run(bootstrap, namespace);
        }
    }
}
