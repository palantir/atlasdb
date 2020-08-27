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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cli.output.OutputPrinter;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.schema.KeyValueServiceMigrator;
import com.palantir.atlasdb.schema.TaskProgress;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.logsafe.Preconditions;
import com.palantir.timestamp.TimestampManagementService;
import org.immutables.value.Value;
import org.slf4j.LoggerFactory;

public final class KeyValueServiceMigrators {
    private static final OutputPrinter printer
            = new OutputPrinter(LoggerFactory.getLogger(KeyValueServiceMigrator.class));

    static final Namespace CHECKPOINT_NAMESPACE = Namespace.create("kvs_migrate");

    private KeyValueServiceMigrators() {
        // utility
    }

    public static KeyValueServiceMigrator setupMigrator(MigratorSpec migratorSpec) {
        AtlasDbServices fromServices = migratorSpec.fromServices();
        AtlasDbServices toServices = migratorSpec.toServices();
        TimestampManagementService toTimestampManagementService = toServices.getManagedTimestampService();

        toTimestampManagementService.fastForwardTimestamp(
                fromServices.getManagedTimestampService().getFreshTimestamp() + 1);
        long migrationStartTimestamp = toServices.getManagedTimestampService().getFreshTimestamp();
        long migrationCommitTimestamp = toServices.getManagedTimestampService().getFreshTimestamp();
        toServices.getTransactionService().putUnlessExists(migrationStartTimestamp, migrationCommitTimestamp);

        return new KeyValueServiceMigrator(
                CHECKPOINT_NAMESPACE,
                fromServices.getTransactionManager(),
                toServices.getTransactionManager(),
                fromServices.getKeyValueService(),
                toServices.getKeyValueService(),
                Suppliers.ofInstance(migrationStartTimestamp),
                migratorSpec.threads(),
                migratorSpec.batchSize(),
                ImmutableMap.of(),
                (String message, KeyValueServiceMigrator.KvsMigrationMessageLevel level) ->
                        printer.info(level.toString() + ": " + message),
                new TaskProgress() {
                    @Override
                    public void beginTask(String message, int tasks) {
                        printer.info(message);
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
                ImmutableSet.of());
    }

    @Value.Immutable
    abstract static class MigratorSpec {
        public abstract AtlasDbServices fromServices();
        public abstract AtlasDbServices toServices();

        @Value.Default
        public int threads() {
            return 16;
        }

        @Value.Default
        public int batchSize() {
            return 100;
        }

        @Value.Check
        void check() {
            Preconditions.checkArgument(threads() > 0, "Threads used for migration should be positive.");
            Preconditions.checkArgument(batchSize() > 0, "Batch size used for migration should be positive.");
        }
    }
}
