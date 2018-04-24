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

import java.util.Objects;

import com.palantir.atlasdb.dropwizard.AtlasDbConfigurationProvider;

import io.dropwizard.Configuration;
import io.dropwizard.cli.ConfiguredCommand;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

public abstract class AtlasDbCommand<T extends Configuration & AtlasDbConfigurationProvider>
        extends ConfiguredCommand<T> {
    private final Class<T> configurationClass;

    protected AtlasDbCommand(String name, String description, Class<T> configurationClass) {
        super(name, description);

        this.configurationClass = configurationClass;
    }

    @Override
    protected Class<T> getConfigurationClass() {
        return configurationClass;
    }

    protected void addOfflineParameter(Subparser subparser) {
        subparser.addArgument(AtlasDbCommandUtils.OFFLINE_COMMAND_ARG_NAME)
                .help("run this cli offline")
                .dest(AtlasDbCommandUtils.OFFLINE_COMMAND_ARG_NAME)
                .required(false)
                .action(Arguments.storeConst())
                .setConst(AtlasDbCommandUtils.ZERO_ARITY_ARG_CONSTANT);
    }

    protected boolean isCliRunningOffline(Namespace namespace) {
        return Objects.equals(
                namespace.getString(AtlasDbCommandUtils.OFFLINE_COMMAND_ARG_NAME),
                AtlasDbCommandUtils.ZERO_ARITY_ARG_CONSTANT);
    }
}
