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
package com.palantir.atlasdb.dropwizard.commands;

import java.util.Collection;
import java.util.List;

import org.apache.commons.cli.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.console.AtlasConsoleMain;
import com.palantir.atlasdb.dropwizard.AtlasDbConfigurationProvider;

import io.dropwizard.Configuration;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

public class AtlasDbConsoleCommand<T extends Configuration & AtlasDbConfigurationProvider> extends AtlasDbCommand<T> {
    private static final Logger log = LoggerFactory.getLogger(AtlasDbConsoleCommand.class);

    public AtlasDbConsoleCommand(Class<T> configurationClass) {
        super("console", "Open an AtlasDB console", configurationClass);
    }

    @Override
    public void configure(Subparser subparser) {
        for (Option option : (Collection<Option>) AtlasConsoleMain.OPTIONS.getOptions()) {
            int numArgs = option.getArgs();
            if (option.getOpt().equals("h")) {
                continue;
            }
            Argument arg = subparser.addArgument("-" + option.getOpt(), "--" + option.getLongOpt())
                    .required(option.isRequired())
                    .help(option.getDescription())
                    .dest("--" + option.getLongOpt());
            if (numArgs == Option.UNLIMITED_VALUES) {
                arg.nargs("+");
            } else if (numArgs != Option.UNINITIALIZED) {
                arg.nargs(numArgs);
            }
        }

        addOfflineParameter(subparser);
        super.configure(subparser);
    }

    @Override
    protected void run(Bootstrap<T> bootstrap, Namespace namespace, T configuration) throws JsonProcessingException {
        AtlasDbConfig cliConfiguration = AtlasDbCommandUtils.convertServerConfigToClientConfig(
                configuration.getAtlasDbConfig(), configuration.getAtlasDbRuntimeConfig());

        // We do this here because there's no flag to connect to an offline
        // cluster in atlasdb-console (since this is passed in through bind)
        if (isCliRunningOffline(namespace)) {
            cliConfiguration = cliConfiguration.toOfflineConfig();
        }

        List<String> allArgs = ImmutableList.<String>builder()
                .add("--bind")
                .add("dropwizardAtlasDb")
                .add(AtlasDbCommandUtils.serialiseConfiguration(cliConfiguration))
                .add("--evaluate")
                .add("connectInline dropwizardAtlasDb")
                .addAll(AtlasDbCommandUtils.gatherPassedInArguments(namespace.getAttrs()))
                .build();

        runAtlasDbConsole(allArgs);
    }

    protected void runAtlasDbConsole(List<String> allArgs) {
        try {
            AtlasConsoleMain.main(allArgs.toArray(new String[] {}));
        } catch (Throwable e) {
            log.error("Error running AtlasDB console", e);
            throw e;
        }
    }
}
