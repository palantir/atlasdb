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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.cli.AtlasCli;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.dropwizard.AtlasDbConfigurationProvider;

import io.airlift.airline.Cli;
import io.airlift.airline.OptionType;
import io.airlift.airline.model.CommandGroupMetadata;
import io.airlift.airline.model.CommandMetadata;
import io.airlift.airline.model.OptionMetadata;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

public class AtlasDbCliCommand<T extends Configuration & AtlasDbConfigurationProvider> extends AtlasDbCommand<T> {
    private static final Logger log = LoggerFactory.getLogger(AtlasDbCliCommand.class);

    private static final String COMMAND_NAME_ATTR = "airlineSubCommand";
    private static final Cli<Callable<?>> CLI = AtlasCli.buildCli();

    public AtlasDbCliCommand(Class<T> configurationClass) {
        super(CLI.getMetadata().getName(), CLI.getMetadata().getDescription(), configurationClass);
    }

    @Override
    public void configure(Subparser subparser) {
        for (CommandMetadata subCommand : CLI.getMetadata().getDefaultGroupCommands()) {
            addCommandToParser(subparser, subCommand, ImmutableList.of());
        }

        for (CommandGroupMetadata commandGroup : CLI.getMetadata().getCommandGroups()) {
            Subparser parser = subparser.addSubparsers()
                    .addParser(commandGroup.getName())
                    .description(commandGroup.getDescription());
            for (CommandMetadata subCommand : commandGroup.getCommands()) {
                addCommandToParser(parser, subCommand, ImmutableList.of(commandGroup.getName()));
            }
        }
    }

    private void addCommandToParser(Subparser subparser, CommandMetadata subCommand, List<String> commandRoot) {
        Subparser parser = subparser.addSubparsers()
                .addParser(subCommand.getName())
                .description(subCommand.getDescription())
                .setDefault(COMMAND_NAME_ATTR, ImmutableList.builder()
                        .addAll(commandRoot)
                        .add(subCommand.getName())
                        .build());

        for (OptionMetadata option : subCommand.getAllOptions()) {
            addOptionToParser(parser, option);
        }

        super.configure(parser);
    }

    @VisibleForTesting
    static void addOptionToParser(Subparser parser, OptionMetadata option) {
        if (option.isHidden()) {
            return;
        }

        List<String> sortedOptions = option.getOptions().stream()
                .sorted((first, second) -> Integer.compareUnsigned(first.length(), second.length()))
                .collect(Collectors.toList());

        String longOption = Iterables.getLast(sortedOptions);

        Argument arg = parser.addArgument(sortedOptions.toArray(new String[] {}))
                .required(option.isRequired())
                .help(option.getDescription())
                .dest(longOption);

        if (option.getArity() == 0) {
            arg.action(Arguments.storeConst());
            arg.setConst(AtlasDbCommandUtils.ZERO_ARITY_ARG_CONSTANT);
        } else {
            arg.nargs(option.getArity());
        }
    }

    @Override
    protected void run(Bootstrap<T> bootstrap, Namespace namespace, T configuration) throws Exception {
        AtlasDbConfig cliConfiguration = AtlasDbCommandUtils.convertServerConfigToClientConfig(
                configuration.getAtlasDbConfig(), configuration.getAtlasDbRuntimeConfig());

        Map<String, OptionType> optionTypes = getCliOptionTypes();

        Map<String, Object> globalAttrs = Maps.filterKeys(namespace.getAttrs(),
                key -> optionTypes.get(key) == OptionType.GLOBAL);
        Map<String, Object> groupAttrs = Maps.filterKeys(namespace.getAttrs(),
                key -> optionTypes.get(key) == OptionType.GROUP);
        Map<String, Object> commandAttrs = Maps.filterKeys(namespace.getAttrs(),
                key -> optionTypes.get(key) == OptionType.COMMAND);

        Iterable<String> groups = Iterables.limit(namespace.getList(COMMAND_NAME_ATTR), 1);
        Iterable<String> commands = Iterables.skip(namespace.getList(COMMAND_NAME_ATTR), 1);

        List<String> allArgs = ImmutableList.<String>builder()
                .add("--inline-config")
                .add(AtlasDbCommandUtils.serialiseConfiguration(cliConfiguration))
                .addAll(AtlasDbCommandUtils.gatherPassedInArguments(globalAttrs))
                .addAll(groups)
                .addAll(AtlasDbCommandUtils.gatherPassedInArguments(groupAttrs))
                .addAll(commands)
                .addAll(AtlasDbCommandUtils.gatherPassedInArguments(commandAttrs))
                .build();

        try {
            CLI.parse(allArgs).call();
        } catch (Throwable e) {
            log.error("Error running AtlasDB CLI", e);
            throw e;
        }
    }

    private static Map<String, OptionType> getCliOptionTypes() {
        Map<String, OptionType> optionTypes = new HashMap<>();

        for (CommandMetadata subCommand : CLI.getMetadata().getDefaultGroupCommands()) {
            optionTypes.putAll(getOptionTypesForCommandMetadata(subCommand));
        }

        for (CommandGroupMetadata commandGroup : CLI.getMetadata().getCommandGroups()) {
            for (CommandMetadata subCommand : commandGroup.getCommands()) {
                optionTypes.putAll(getOptionTypesForCommandMetadata(subCommand));
            }
        }

        return ImmutableMap.copyOf(optionTypes);
    }

    @VisibleForTesting
    static Map<String, OptionType> getOptionTypesForCommandMetadata(CommandMetadata subCommand) {
        Map<String, OptionType> optionTypes = new HashMap<>();
        List<OptionMetadata> commandOptions = ImmutableList.<OptionMetadata>builder()
                .addAll(subCommand.getCommandOptions())
                .addAll(subCommand.getGroupOptions())
                .addAll(subCommand.getGlobalOptions())
                .build();

        for (OptionMetadata option : commandOptions) {
            if (option.isHidden()) {
                continue;
            }

            List<String> sortedOptions = option.getOptions().stream()
                    .sorted((first, second) -> Integer.compareUnsigned(first.length(), second.length()))
                    .collect(Collectors.toList());

            String longOption = Iterables.getLast(sortedOptions);

            optionTypes.put(longOption, option.getOptionType());
        }

        return ImmutableMap.copyOf(optionTypes);
    }
}
