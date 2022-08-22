/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.cli.command;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cli.output.OutputPrinter;
import com.palantir.atlasdb.compact.CompactorConfig;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbConfigs;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.SweepConfig;
import com.palantir.atlasdb.schema.KeyValueServiceMigrator;
import com.palantir.atlasdb.schema.KeyValueServiceValidator;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.services.DaggerAtlasDbServices;
import com.palantir.atlasdb.services.ServicesConfigModule;
import com.palantir.atlasdb.sweep.queue.config.TargetedSweepRuntimeConfig;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.slf4j.LoggerFactory;

@Command(name = "migrate", description = "Migrate your data from one key value service to another.")
public class KvsMigrationCommand implements Callable<Integer> {
    private static final OutputPrinter printer = new OutputPrinter(LoggerFactory.getLogger(KvsMigrationCommand.class));
    private static final int TRANSACTION_READ_TIMEOUT_MILLIS_OVERRIDE = 8 * 60 * 60 * 1000;

    @Option(
            name = {"-fc", "--fromConfig"},
            title = "CONFIG PATH",
            description = "path to yaml configuration file for the KVS you're migrating from",
            required = true)
    private File fromConfigFile;

    @Option(
            name = {"-mc", "--migrateConfig"},
            title = "CONFIG PATH",
            description = "path to yaml configuration file for the KVS you're migrating to",
            required = false)
    private File toConfigFile;

    @Option(
            name = {"--config-root"},
            title = "INSTALL CONFIG ROOT",
            type = OptionType.GLOBAL,
            description = "field in the config yaml file that contains the atlasdb configuration root")
    private String configRoot = AtlasDbConfigs.ATLASDB_CONFIG_OBJECT_PATH;

    @Option(
            name = {"-frc", "--fromRuntimeConfig"},
            title = "RUNTIME CONFIG PATH",
            description = "path to yaml runtime configuration file for the KVS you're migrating from. Note that this "
                    + "config will not be reloaded while the CLI is running",
            required = false)
    private File fromRuntimeConfigFile;

    @Option(
            name = {"-mrc", "--migrateRuntimeConfig"},
            title = "RUNTIME CONFIG PATH",
            description = "path to yaml runtime configuration file for the KVS you're migrating to. Note that this "
                    + "config will not be reloaded while the CLI is running",
            required = false)
    private File toRuntimeConfigFile;

    @Option(
            name = {"--runtime-config-root"},
            title = "RUNTIME CONFIG ROOT",
            type = OptionType.GLOBAL,
            description = "field in the runtime config yaml file that contains the atlasdb configuration root")
    private String runtimeConfigRoot = AtlasDbConfigs.ATLASDB_CONFIG_OBJECT_PATH;

    @Option(
            name = {"-t", "--threads"},
            title = "THREADS",
            description = "number of threads to use for migration",
            required = false,
            arity = 1)
    private int threads = 16;

    @Option(
            name = {"-b", "--batchSize"},
            title = "BATCH SIZE",
            description = "batch size of rows to read",
            required = false,
            arity = 1)
    private int batchSize = 100;

    @Option(
            name = {"-s", "--setup"},
            description = "Setup migration by dropping and creating tables.")
    private boolean setup = false;

    @Option(
            name = {"-m", "--migrate"},
            description = "Start or continue migration.")
    private boolean migrate = false;

    @Option(
            name = {"-v", "--validate"},
            description = "Validate migration.")
    private boolean validate = false;

    @Option(
            name = {"--offline"},
            title = "OFFLINE",
            type = OptionType.GLOBAL,
            description = "run this cli offline")
    private boolean offline = false;

    // TODO(bgrabham): Hide this argument once https://github.com/airlift/airline/issues/51 is fixed
    @Option(
            name = {"--inline-config"},
            title = "INLINE INSTALL CONFIG",
            type = OptionType.GLOBAL,
            description = "inline configuration file for atlasdb")
    private String inlineConfig;

    @Option(
            name = {"--inline-runtime-config"},
            title = "INLINE RUNTIME CONFIG",
            type = OptionType.GLOBAL,
            description = "inline runtime configuration file for atlasdb")
    private String inlineRuntimeConfig;

    @Override
    public Integer call() throws Exception {
        if (inlineConfig == null && toConfigFile == null) {
            printer.error("Argument -mc/--migrateConfig is required when not running as a dropwizard CLI");
            return 1;
        }

        AtlasDbServices fromServices = connectFromServices();
        AtlasDbServices toServices = connectToServices();
        return execute(fromServices, toServices);
    }

    public int execute(AtlasDbServices fromServices, AtlasDbServices toServices) {
        if (!setup && !migrate && !validate) {
            printer.error("At least one of --setup, --migrate, or --validate should be specified.");
            return 1;
        }

        KeyValueServiceMigrator migrator;
        migrator = getMigrator(fromServices, toServices);
        if (setup) {
            migrator.setup();
        }
        if (migrate) {
            migrator.migrate();

            migrator.cleanup();
        }
        if (validate) {
            KeyValueServiceValidator validator = new KeyValueServiceValidator(
                    fromServices.getTransactionManager(),
                    toServices.getTransactionManager(),
                    fromServices.getKeyValueService(),
                    threads,
                    batchSize,
                    ImmutableMap.of(),
                    (String message, KeyValueServiceMigrator.KvsMigrationMessageLevel level) ->
                            printer.info(level.toString() + ": " + message),
                    ImmutableSet.of());
            validator.validate(true);
        }
        return 0;
    }

    private AtlasDbConfig makeOfflineIfNecessary(AtlasDbConfig atlasDbConfig) {
        if (offline) {
            return atlasDbConfig.toOfflineConfig();
        } else {
            return atlasDbConfig;
        }
    }

    public AtlasDbServices connectFromServices() {
        AtlasDbConfig fromConfig = overrideTransactionTimeoutMillis(
                loadFromFile(fromConfigFile, configRoot, AtlasDbConfig.class).orElseThrow());

        AtlasDbRuntimeConfig fromRuntimeConfig = loadFromFile(
                        fromRuntimeConfigFile, runtimeConfigRoot, AtlasDbRuntimeConfig.class)
                .map(KvsMigrationCommand::disableSweepAndCompaction)
                .orElseGet(AtlasDbRuntimeConfig::withSweepDisabled);

        ServicesConfigModule scm = ServicesConfigModule.create(makeOfflineIfNecessary(fromConfig), fromRuntimeConfig);
        return DaggerAtlasDbServices.builder().servicesConfigModule(scm).build();
    }

    public AtlasDbServices connectToServices() {
        AtlasDbConfig toConfig = overrideTransactionTimeoutMillis(loadFromFileOrInline(
                        toConfigFile, configRoot, inlineConfig, AtlasDbConfig.class)
                .orElseThrow(() -> new SafeRuntimeException("At least one of -mc / --inline-config is required")));

        AtlasDbRuntimeConfig toRuntimeConfig = loadFromFileOrInline(
                        toRuntimeConfigFile, runtimeConfigRoot, inlineRuntimeConfig, AtlasDbRuntimeConfig.class)
                .map(KvsMigrationCommand::disableSweepAndCompaction)
                .orElseGet(AtlasDbRuntimeConfig::withSweepDisabled);
        ServicesConfigModule scm = ServicesConfigModule.create(makeOfflineIfNecessary(toConfig), toRuntimeConfig);
        return DaggerAtlasDbServices.builder().servicesConfigModule(scm).build();
    }

    private AtlasDbConfig overrideTransactionTimeoutMillis(AtlasDbConfig config) {
        return ImmutableAtlasDbConfig.builder()
                .from(config)
                .transactionReadTimeoutMillis(TRANSACTION_READ_TIMEOUT_MILLIS_OVERRIDE)
                .build();
    }

    private KeyValueServiceMigrator getMigrator(AtlasDbServices fromServices, AtlasDbServices toServices) {
        return KeyValueServiceMigrators.setupMigrator(ImmutableMigratorSpec.builder()
                .fromServices(fromServices)
                .toServices(toServices)
                .threads(threads)
                .batchSize(batchSize)
                .build());
    }

    private static AtlasDbRuntimeConfig disableSweepAndCompaction(AtlasDbRuntimeConfig atlasDbRuntimeConfig) {
        return ImmutableAtlasDbRuntimeConfig.builder()
                .from(atlasDbRuntimeConfig)
                .sweep(SweepConfig.disabled())
                .targetedSweep(TargetedSweepRuntimeConfig.disabled())
                .compact(CompactorConfig.disabled())
                .build();
    }

    private static <K> Optional<K> loadFromFileOrInline(
            @Nullable File configFile, String fileConfigRoot, @Nullable String inline, Class<K> clazz) {
        return loadFromFile(configFile, fileConfigRoot, clazz).or(() -> Optional.ofNullable(inline)
                .map(rethrowIoExceptionAsUnchecked(config -> AtlasDbConfigs.loadFromString(config, null, clazz))));
    }

    private static <K> Optional<K> loadFromFile(@Nullable File configFile, String fileConfigRoot, Class<K> clazz) {
        return Optional.ofNullable(configFile)
                .map(rethrowIoExceptionAsUnchecked(file -> AtlasDbConfigs.load(file, fileConfigRoot, clazz)));
    }

    private static <F, T, K extends IOException> Function<F, T> rethrowIoExceptionAsUnchecked(
            FunctionCheckedException<F, T, K> function) {
        return f -> {
            try {
                return function.apply(f);
            } catch (IOException e) {
                throw new SafeRuntimeException(e);
            }
        };
    }
}
