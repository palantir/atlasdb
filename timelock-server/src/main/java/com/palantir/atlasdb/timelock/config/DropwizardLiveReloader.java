/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.config;

import java.io.File;
import java.io.IOException;

import com.google.common.base.Throwables;

import io.dropwizard.configuration.ConfigurationException;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.setup.Bootstrap;

public class DropwizardLiveReloader {
    private final ConfigurationFactory<TimeLockServerConfiguration> configurationFactory;
    private final File configurationFile;

    private DropwizardLiveReloader(
            ConfigurationFactory<TimeLockServerConfiguration> configurationFactory,
            File configurationFile) {
        this.configurationFactory = configurationFactory;
        this.configurationFile = configurationFile;
    }

    public static DropwizardLiveReloader create(
            Bootstrap<TimeLockServerConfiguration> configuration,
            File configurationFile) {
        return new DropwizardLiveReloader(
                configuration.getConfigurationFactoryFactory().create(TimeLockServerConfiguration.class,
                        configuration.getValidatorFactory().getValidator(),
                        configuration.getObjectMapper(),
                        ""),
                configurationFile);
    }

    public TimeLockServerConfiguration getNewConfiguration() {
        try {
            return configurationFactory.build(configurationFile);
        } catch (IOException | ConfigurationException e) {
            throw Throwables.propagate(e);
        }
    }
}
