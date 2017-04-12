/*
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
package com.palantir.atlasdb.timelock;

import java.io.File;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.config.crypto.DecryptingVariableSubstitutor;
import com.palantir.config.crypto.SubstitutingConfigurationFactory;
import com.palantir.config.crypto.jackson.JsonNodeStringReplacer;

import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.ConfigurationSourceProvider;
import io.dropwizard.configuration.FileConfigurationSourceProvider;
import io.dropwizard.setup.Environment;

public final class ConfigurationWatcher implements Supplier<TimeLockServerConfiguration> {
    private final AtomicReference<TimeLockServerConfiguration> configuration;

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationWatcher.class);

    public ConfigurationWatcher(
            TimeLockServerConfiguration defaultConfiguration,
            Environment environment,
            String configurationFilePath) {

        this(defaultConfiguration, environment, new File(configurationFilePath), 5, TimeUnit.SECONDS);
    }

    public ConfigurationWatcher(
            TimeLockServerConfiguration defaultConfiguration,
            Environment environment,
            File configurationFile,
            long period,
            TimeUnit unit) {

        this.configuration = new AtomicReference<>(defaultConfiguration);

        ScheduledExecutorService executorService =
                environment.lifecycle().scheduledExecutorService("reload-%d", true).build();

        new FileWatcher(configurationFile, period, unit, executorService, () -> {
            LOGGER.info("Configuration change detected.");
            this.parse(configurationFile, environment, new FileConfigurationSourceProvider());
        });
    }

    @Override
    public TimeLockServerConfiguration get() {
        return this.configuration.get();
    }

    @VisibleForTesting
    boolean parse(File configurationFile, Environment environment,
            ConfigurationSourceProvider configurationSourceProvider) {
        try {
            JsonNodeStringReplacer replacer = new JsonNodeStringReplacer(new DecryptingVariableSubstitutor());
            SubstitutingConfigurationFactory.Factory<TimeLockServerConfiguration> configurationFactoryFactory =
                    new SubstitutingConfigurationFactory.Factory<>(replacer);

            ConfigurationFactory<TimeLockServerConfiguration> configurationFactory = configurationFactoryFactory.create(
                    TimeLockServerConfiguration.class,
                    environment.getValidator(),
                    environment.getObjectMapper(),
                    "dw");
            ConfigurationSourceProvider provider = configurationSourceProvider;
            TimeLockServerConfiguration newConfiguration = configurationFactory.build(provider,
                    configurationFile.getAbsolutePath());

            this.configuration.set(newConfiguration);

            LOGGER.info("Reloaded configuration file.");
            return true;
        } catch (Exception exception) {
            LOGGER.warn("Invalid configuration file, old configuration will be preserved.", exception);
        }

        return false;
    }

    @VisibleForTesting
    static final class FileWatcher {
        private final AtomicLong lastModified = new AtomicLong(0);
        private final File file;
        private final Runnable handler;

        FileWatcher(
                File file,
                long period,
                TimeUnit unit,
                ScheduledExecutorService executorService,
                Runnable handler) {

            this.file = file;
            this.handler = handler;

            executorService.scheduleAtFixedRate(this::checkForChanges, 0, period, unit);
        }

        @VisibleForTesting
        void checkForChanges() {
            long fileLastModified = file.lastModified();
            long savedLastModified = lastModified.longValue();
            if (fileLastModified > savedLastModified) {
                lastModified.set(fileLastModified);
                if (savedLastModified != 0) {
                    handler.run();
                }
            }
        }
    }
}
