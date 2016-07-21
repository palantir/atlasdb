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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.cli.AtlasCli;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.dropwizard.AtlasDbConfigurationProvider;

import io.airlift.airline.Cli;
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
    private static final String COMMAND_NAME_ATTR = "airlineSubCommand";
    private static final Object ZERO_ARITY_ARG_CONSTANT = "<ZERO ARITY ARG CONSTANT>";
    private static final Cli<Callable> CLI = AtlasCli.buildCli();
    private static final ObjectMapper OBJECT_MAPPER;

    static {
        YAMLFactory yamlFactory = new YAMLFactory();
        yamlFactory.configure(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID, false);
        OBJECT_MAPPER = new ObjectMapper(yamlFactory);
        OBJECT_MAPPER.registerModule(new GuavaModule());
    }

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
                .setDefault(COMMAND_NAME_ATTR, ImmutableList.builder().addAll(commandRoot).add(subCommand.getName()).build());

        List<OptionMetadata> commandOptions = ImmutableList.<OptionMetadata>builder()
                .addAll(subCommand.getCommandOptions())
                .addAll(subCommand.getGroupOptions())
                .build();

        for (OptionMetadata option : commandOptions) {
            if (option.isHidden()) {
                continue;
            }

            List<String> sortedOptions = option.getOptions().stream()
                    .sorted((a, b) -> Integer.compareUnsigned(a.length(), b.length()))
                    .collect(Collectors.toList());

            String longOption = Iterables.getLast(sortedOptions);

            Argument arg = parser.addArgument(sortedOptions.toArray(new String[] {}))
                    .required(option.isRequired())
                    .help(option.getDescription())
                    .dest(longOption);

            if(option.getArity() == 0) {
                arg.action(Arguments.storeConst());
                arg.setConst(ZERO_ARITY_ARG_CONSTANT);
            } else {
                arg.nargs(option.getArity());
            }
        }

        super.configure(parser);
    }

    @Override
    protected void run(Bootstrap<T> bootstrap, Namespace namespace, T configuration) throws Exception {
        AtlasDbConfig configurationWithoutLeader = ImmutableAtlasDbConfig.builder()
                .from(configuration.getAtlasDbConfig())
                .leader(Optional.absent())
                .build();

        List<String> passedInArgs = namespace.getAttrs().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith("--"))
                .filter(entry -> entry.getValue() != null)
                .flatMap(entry -> {
                    if (entry.getValue() instanceof List) {
                        return Stream.concat(Stream.of(entry.getKey()), ((List<String>) entry.getValue()).stream());
                    } else if (entry.getValue().equals(ZERO_ARITY_ARG_CONSTANT)) {
                        return Stream.of(entry.getKey());
                    } else {
                        return Stream.of(entry.getKey(), (String) entry.getValue());
                    }
                })
                .collect(Collectors.toList());

        List<String> allArgs = ImmutableList.<String>builder()
                .add("--inline-config")
                .add(OBJECT_MAPPER.writeValueAsString(configurationWithoutLeader))
                .addAll(namespace.getList(COMMAND_NAME_ATTR))
                .addAll(passedInArgs)
                .build();

        CLI.parse(allArgs).call();
    }
}
