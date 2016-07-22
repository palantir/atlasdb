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

import static org.apache.commons.cli.Option.UNINITIALIZED;
import static org.apache.commons.cli.Option.UNLIMITED_VALUES;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.cli.Option;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.console.AtlasConsoleMain;
import com.palantir.atlasdb.dropwizard.AtlasDbConfigurationProvider;

import io.dropwizard.Configuration;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

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
    public void configure(Subparser subparser) {
        super.configure(subparser);

        for (Option option : (Collection<Option>) AtlasConsoleMain.OPTIONS.getOptions()) {
            int numArgs = option.getArgs();
            if(option.getOpt().equals("h")) {
                continue;
            }
            Argument arg = subparser.addArgument("-" + option.getOpt(), "--" + option.getLongOpt())
                    .required(option.isRequired())
                    .help(option.getDescription())
                    .dest("--" + option.getLongOpt());
            if (numArgs == UNLIMITED_VALUES) {
                arg.nargs("+");
            } else if (numArgs != UNINITIALIZED) {
                arg.nargs(numArgs);
            }
        }
    }

    @Override
    protected void run(Bootstrap<T> bootstrap, Namespace namespace, T configuration) throws Exception {
        Preconditions.checkArgument(configuration.getAtlasDbConfig().leader().isPresent(), "CLIs can only be run with a leader block");

        ServerListConfig leaders = ImmutableServerListConfig.builder()
                .servers(configuration.getAtlasDbConfig().leader().get().leaders())
                .build();

        AtlasDbConfig cliConfiguration = ImmutableAtlasDbConfig.builder()
                .from(configuration.getAtlasDbConfig())
                .leader(Optional.absent())
                .lock(leaders)
                .timestamp(leaders)
                .build();

        List<String> passedInArgs = namespace.getAttrs().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith("--"))
                .filter(entry -> entry.getValue() != null)
                .flatMap(entry -> {
                    if (entry.getValue() instanceof List) {
                        return Stream.concat(Stream.of(entry.getKey()), ((List<String>) entry.getValue()).stream());
                    } else {
                        return Stream.of(entry.getKey(), (String) entry.getValue());
                    }
                })
                .collect(Collectors.toList());

        List<String> allArgs = ImmutableList.<String>builder()
                .add("--bind")
                .add("dropwizardAtlasDb")
                .add(OBJECT_MAPPER.writeValueAsString(cliConfiguration))
                .add("--evaluate")
                .add("connectInline dropwizardAtlasDb")
                .addAll(passedInArgs)
                .build();


        AtlasConsoleMain.main(allArgs.toArray(new String[] {}));
    }
}
