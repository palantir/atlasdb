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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.airlift.airline.OptionType;
import io.airlift.airline.model.ArgumentsMetadata;
import io.airlift.airline.model.CommandMetadata;
import io.airlift.airline.model.OptionMetadata;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Subparser;

public class AtlasDbCliCommandTest {
    private static final String GLOBAL_OPTION_LONG_NAME = "--global";
    private static final String GLOBAL_OPTION_SHORT_NAME = "-g";
    private static final OptionMetadata GLOBAL_OPTION =
            createOption(OptionType.GLOBAL, ImmutableSet.of(GLOBAL_OPTION_SHORT_NAME, GLOBAL_OPTION_LONG_NAME));

    private static final String GROUP_OPTION_NAME = "--group";
    private static final OptionMetadata GROUP_OPTION =
            createOption(OptionType.GROUP, ImmutableSet.of(GROUP_OPTION_NAME));

    private static final String COMMAND_OPTION_NAME = "--command";
    private static final OptionMetadata COMMAND_OPTION =
            createOption(OptionType.COMMAND, ImmutableSet.of(COMMAND_OPTION_NAME));

    // This is only used for passing in a valid field when creating an option
    @SuppressWarnings("unused")
    private String optionField;

    @Test
    public void commandOptionMapsToCommandType() {
        Map<String, OptionType> expectedOptionsToCommandType = ImmutableMap.of(COMMAND_OPTION_NAME, OptionType.COMMAND);
        Map<String, OptionType> actualOptionsToCommandType = AtlasDbCliCommand.getOptionTypesForCommandMetadata(
                createCommand(ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of(COMMAND_OPTION)));

        assertThat(actualOptionsToCommandType).isEqualTo(expectedOptionsToCommandType);
    }

    @Test
    public void groupOptionMapsToGroupType() {
        Map<String, OptionType> expectedOptionsToCommandType = ImmutableMap.of(GROUP_OPTION_NAME, OptionType.GROUP);
        Map<String, OptionType> actualOptionsToCommandType = AtlasDbCliCommand.getOptionTypesForCommandMetadata(
                createCommand(ImmutableSet.of(), ImmutableSet.of(GROUP_OPTION), ImmutableSet.of()));

        assertThat(actualOptionsToCommandType).isEqualTo(expectedOptionsToCommandType);
    }

    @Test
    public void globalOptionMapsToGlobalType() {
        Map<String, OptionType> expectedOptionsToCommandType =
                ImmutableMap.of(GLOBAL_OPTION_LONG_NAME, OptionType.GLOBAL);
        Map<String, OptionType> actualOptionsToCommandType = AtlasDbCliCommand.getOptionTypesForCommandMetadata(
                createCommand(ImmutableSet.of(GLOBAL_OPTION), ImmutableSet.of(), ImmutableSet.of()));

        assertThat(actualOptionsToCommandType).isEqualTo(expectedOptionsToCommandType);
    }

    @Test
    public void globalOptionMapsToLongOptionName() {
        Map<String, OptionType> actualOptionsToCommandType = AtlasDbCliCommand.getOptionTypesForCommandMetadata(
                createCommand(ImmutableSet.of(GLOBAL_OPTION), ImmutableSet.of(), ImmutableSet.of()));

        assertThat(actualOptionsToCommandType).containsOnlyKeys(GLOBAL_OPTION_LONG_NAME);
    }

    @Test
    public void helpIsSetOnArgument() {
        String expectedDescription = "description";

        OptionMetadata metadata = new OptionMetadata(
                OptionType.COMMAND,
                ImmutableSet.of("arg"),
                "name",
                expectedDescription,
                0,
                false,
                false,
                ImmutableSet.of(),
                getOptionFields());
        Argument argument = getArgumentMockFromAddOptionToParser(metadata, "arg");

        verify(argument).help(expectedDescription);
    }

    @Test
    public void requiredIsSetOnArgument() {
        boolean expectedRequired = true;

        OptionMetadata metadata = new OptionMetadata(
                OptionType.COMMAND,
                ImmutableSet.of("arg"),
                "name",
                "description",
                0,
                expectedRequired,
                false,
                ImmutableSet.of(),
                getOptionFields());
        Argument argument = getArgumentMockFromAddOptionToParser(metadata, "arg");

        verify(argument).required(expectedRequired);
    }

    @Test
    public void zeroArityConstantIsSetOnArgumentWhenThereArentAnyExpectedArguments() {
        OptionMetadata metadata = new OptionMetadata(
                OptionType.COMMAND,
                ImmutableSet.of("arg"),
                "name",
                "description",
                0,
                false,
                false,
                ImmutableSet.of(),
                getOptionFields());
        Argument argument = getArgumentMockFromAddOptionToParser(metadata, "arg");

        verify(argument).action(Arguments.storeConst());
        verify(argument).setConst(AtlasDbCommandUtils.ZERO_ARITY_ARG_CONSTANT);
    }

    @Test
    public void nargsIsSetOnArgumentWhenThereAreExpectedArguments() {
        int expectedArity = 5;

        OptionMetadata metadata = new OptionMetadata(
                OptionType.COMMAND,
                ImmutableSet.of("arg"),
                "name",
                "description",
                expectedArity,
                false,
                false,
                ImmutableSet.of(),
                getOptionFields());
        Argument argument = getArgumentMockFromAddOptionToParser(metadata, "arg");

        verify(argument).nargs(5);
    }

    @Test
    public void destIsSetToLongestArg() {
        String expectedDest = "long string here";

        OptionMetadata metadata = new OptionMetadata(
                OptionType.COMMAND,
                ImmutableSet.of("arg", "longer arg", expectedDest),
                "name",
                "description",
                0,
                false,
                false,
                ImmutableSet.of(),
                getOptionFields());
        Argument argument = getArgumentMockFromAddOptionToParser(metadata, "arg", "longer arg", expectedDest);

        verify(argument).dest(expectedDest);
    }

    private static Argument getArgumentMockFromAddOptionToParser(OptionMetadata metadata, String... argNames) {
        Subparser parser = mock(Subparser.class);
        Argument argument = mock(Argument.class, (Answer<Object>) (invocation) -> {
            Object mock = invocation.getMock();
            if (invocation.getMethod().getReturnType().isInstance(mock)) {
                return mock;
            } else {
                return Mockito.RETURNS_DEFAULTS.answer(invocation);
            }
        });
        when(parser.addArgument(argNames))
                .thenReturn(argument);
        AtlasDbCliCommand.addOptionToParser(parser, metadata);

        return argument;
    }

    private static CommandMetadata createCommand(
            Iterable<OptionMetadata> globalOptions,
            Iterable<OptionMetadata> groupOptions,
            Iterable<OptionMetadata> commandOptions) {
        return new CommandMetadata(
                "test", "A test command", false,
                globalOptions, groupOptions, commandOptions,
                mock(ArgumentsMetadata.class),
                ImmutableSet.of(), Void.class);
    }

    private static OptionMetadata createOption(OptionType optionType, Iterable<String> options) {
        return new OptionMetadata(
                optionType,
                options,
                "Option",
                "Option",
                0,
                false,
                false,
                ImmutableSet.of(),
                getOptionFields());
    }

    private static Set<Field> getOptionFields() {
        try {
            return ImmutableSet.of(AtlasDbCliCommandTest.class.getDeclaredField("optionField"));
        } catch (NoSuchFieldException e) {
            throw Throwables.propagate(e);
        }
    }
}
