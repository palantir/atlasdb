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
import com.palantir.atlasdb.cli.command.timestamp.CleanTransactionRange;
import com.palantir.atlasdb.cli.command.timestamp.FastForwardTimestamp;
import com.palantir.atlasdb.cli.command.timestamp.FetchTimestamp;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.dropwizard.AtlasDbConfigurationProvider;

import io.airlift.airline.Cli;
import io.airlift.airline.model.CommandMetadata;
import io.airlift.airline.model.OptionMetadata;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

public class AtlasDbTimestampCommand<T extends Configuration & AtlasDbConfigurationProvider> extends AtlasDbCommand<T> {
    private static final String COMMAND_NAME_ATTR = "timestampSubCommand";
    private static final Object ZERO_ARITY_ARG_CONSTANT = "<ZERO ARITY ARG CONSTANT>";
    private static final Cli<Callable> TIMESTAMP_CLI = Cli.<Callable>builder("timestamp")
            .withDescription("Timestamp-centric commands")
            .withCommands(
                    FetchTimestamp.class,
                    CleanTransactionRange.class,
                    FastForwardTimestamp.class)
            .build();
    private static final ObjectMapper OBJECT_MAPPER;

    static {
        YAMLFactory yamlFactory = new YAMLFactory();
        yamlFactory.configure(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID, false);
        OBJECT_MAPPER = new ObjectMapper(yamlFactory);
        OBJECT_MAPPER.registerModule(new GuavaModule());
    }

    public AtlasDbTimestampCommand(Class<T> configurationClass) {
        super("timestamp", "Get timestamp information", configurationClass);
    }

    @Override
    public void configure(Subparser subparser) {
        for (CommandMetadata subCommand : TIMESTAMP_CLI.getMetadata().getDefaultGroupCommands()) {
            Subparser parser = subparser.addSubparsers()
                    .addParser(subCommand.getName())
                    .setDefault(COMMAND_NAME_ATTR, subCommand.getName())
                    .description(subCommand.getDescription());

            List<OptionMetadata> commandOptions = ImmutableList.<OptionMetadata>builder()
                    .addAll(subCommand.getCommandOptions())
                    .addAll(subCommand.getGroupOptions())
                    .build();

            for (OptionMetadata option : commandOptions) {
                if (option.isHidden()) {
                    continue;
                }

                List<String> sortedList = option.getOptions().stream()
                        .sorted((a, b) -> Integer.compareUnsigned(a.length(), b.length()))
                        .collect(Collectors.toList());

                String shortOption = sortedList.get(0);
                String longOption = sortedList.get(1);

                Argument arg = parser.addArgument(shortOption, longOption)
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
                .add(namespace.getString(COMMAND_NAME_ATTR))
                .addAll(passedInArgs)
                .build();

        TIMESTAMP_CLI.parse(allArgs).call();
    }
}
