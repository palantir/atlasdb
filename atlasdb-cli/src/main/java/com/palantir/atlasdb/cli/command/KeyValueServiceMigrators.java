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

import org.immutables.value.Value;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cli.output.OutputPrinter;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.schema.KeyValueServiceMigrator;
import com.palantir.atlasdb.schema.TaskProgress;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;

public final class KeyValueServiceMigrators {
    private static final OutputPrinter printer
            = new OutputPrinter(LoggerFactory.getLogger(KeyValueServiceMigrator.class));

    private static final Namespace CHECKPOINT_NAMESPACE = Namespace.create("kvs_migrate");

    private KeyValueServiceMigrators() {
        // utility
    }

    public static KeyValueServiceMigrator setupMigrator(MigratorSpec migratorSpec) {
        AtlasDbServices fromServices = migratorSpec.fromServices();
        long migrationStartTimestamp = fromServices.getTimestampService().getFreshTimestamp();
        long migrationCommitTimestamp = fromServices.getTimestampService().getFreshTimestamp();

        AtlasDbServices toServices = migratorSpec.toServices();
        TimestampManagementService toTimestampManagementService = getTimestampManagementService(toServices);

        toServices.getTransactionService().putUnlessExists(migrationStartTimestamp, migrationCommitTimestamp);
        toTimestampManagementService.fastForwardTimestamp(migrationCommitTimestamp + 1);

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

    @VisibleForTesting
    static TimestampManagementService getTimestampManagementService(AtlasDbServices toServices) {
        TimestampService toTimestampService = toServices.getTimestampService();
        if (toTimestampService instanceof TimestampManagementService) {
            return (TimestampManagementService) toTimestampService;
        }
        String errorMessage = String.format("Timestamp service must be of type %s, but yours is %s. Exiting.",
                TimestampManagementService.class.toString(),
                toTimestampService.getClass().toString());
        printer.error(errorMessage);
        throw new IllegalArgumentException(errorMessage);
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
