/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.cli.command;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cli.services.AtlasDbServices;
import com.palantir.atlasdb.cli.services.DaggerAtlasDbServices;
import com.palantir.atlasdb.cli.services.ServicesConfigModule;
import com.palantir.atlasdb.config.AtlasDbConfigs;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.KeyValueServiceMigrator;
import com.palantir.atlasdb.schema.KeyValueServiceValidator;
import com.palantir.atlasdb.schema.TaskProgress;
import com.palantir.common.base.Throwables;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

@Command(name = "migrate", description = "Migrate your data from one key value service to another.")
public class KvsMigrationCommand implements Callable<Integer> {

    private static final Logger log = LoggerFactory.getLogger(KvsMigrationCommand.class);

    @Option(name = {"-fc", "--fromConfig"},
            title = "CONFIG PATH",
            description = "path to yaml configuration file for the KVS you're migrating from",
            required = true)
    private File fromConfigFile;

    @Option(name = {"-mc", "--migrateConfig"},
            title = "CONFIG PATH",
            description = "path to yaml configuration file for the KVS you're migrating to",
            required = true)
    private File toConfigFile;

    @Option(name = {"-r", "--config-root"},
            title = "CONFIG ROOT",
            description = "field in the config yaml file that contains the atlasdb configuration root")
    private String configRoot = AtlasDbConfigs.ATLASDB_CONFIG_ROOT;

    @Option(name = {"-t", "--threads"},
            title = "THREADS",
            description = "number of threads to use for migration",
            required = false,
            arity = 1)
    private int threads = 16;

    @Option(name = {"-b", "--batchSize"},
            title = "BATCH SIZE",
            description = "batch size of rows to read",
            required = false,
            arity = 1)
    private int batchSize = 100;

    @Option(name = {"-s", "--setup"},
            description = "Setup migration by dropping and creating tables.")
    private boolean setup = false;

    @Option(name = {"-m", "--migrate"},
            description = "Start or continue migration.")
    private boolean migrate = false;

    @Option(name = {"-v", "--validate"},
            description = "Validate migration.")
    private boolean validate = false;

    private static final Namespace CHECKPOINT_NAMESPACE = Namespace.create("kvs_migrate");

    @Override
    public Integer call() throws Exception {
        AtlasDbServices fromServices = connectFromServices();
        AtlasDbServices toServices = connectToServices();
        return execute(fromServices, toServices);
    }

	public int execute(AtlasDbServices fromServices, AtlasDbServices toServices) {
        if (!setup && !migrate && !validate) {
            log.error("At least one of --setup, --migrate, or --validate should be specified.");
            return 1;
        }
        KeyValueServiceMigrator migrator;
        try {
            migrator = getMigrator(fromServices, toServices);
        } catch (IOException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
        if (setup) {
            migrator.setup();
        }
        if (migrate) {
            migrator.migrate();
        }
        if (validate) {
            KeyValueServiceValidator validator = new KeyValueServiceValidator(fromServices.getTransactionManager(),
                    toServices.getTransactionManager(),
                    fromServices.getKeyValueService(),
                    threads,
                    batchSize,
                    ImmutableMap.<TableReference, Integer>of(),
                    (String message, KeyValueServiceMigrator.KvsMigrationMessageLevel level) -> {
                        log.info(level.toString() + ": " + message);
                    },
                    ImmutableSet.<TableReference>of());
            validator.validate(true);
        }
        return 0;
	}

    public AtlasDbServices connectFromServices() throws IOException {
        ServicesConfigModule scm = ServicesConfigModule.create(fromConfigFile, configRoot);
        return DaggerAtlasDbServices.builder().servicesConfigModule(scm).build();
    }

    public AtlasDbServices connectToServices() throws IOException {
        ServicesConfigModule scm = ServicesConfigModule.create(toConfigFile, configRoot);
        return DaggerAtlasDbServices.builder().servicesConfigModule(scm).build();
    }

    private KeyValueServiceMigrator getMigrator(AtlasDbServices fromServices, AtlasDbServices toServices) throws IOException {
        //TODO: this timestamp will have to be stored for online migration
        Supplier<Long> migrationTimestampSupplier = Suppliers.ofInstance(toServices.getTimestampService().getFreshTimestamp());
        toServices.getTransactionService().putUnlessExists(migrationTimestampSupplier.get(), toServices.getTimestampService().getFreshTimestamp());
        return new KeyValueServiceMigrator(
                CHECKPOINT_NAMESPACE,
                fromServices.getTransactionManager(),
                toServices.getTransactionManager(),
                fromServices.getKeyValueService(),
                toServices.getKeyValueService(),
                migrationTimestampSupplier,
                threads,
                batchSize,
                ImmutableMap.<TableReference, Integer>of(),
                (String message, KeyValueServiceMigrator.KvsMigrationMessageLevel level) -> {
                    log.info(level.toString() + ": " + message);
                },
                new TaskProgress() {
                    @Override
                    public void beginTask(String message, int tasks) {
                        log.info(message);
                    }

                    @Override
                    public void subTaskComplete() {
                        //
                    }

                    @Override
                    public void taskComplete() {
                        //
                    }
                },
                ImmutableSet.<TableReference>of());
    }
}
