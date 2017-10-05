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

package com.palantir.atlasdb.factory;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.schema.SweepSchema;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.common.annotation.Idempotent;

public final class TransactionManagersInitializer extends AsyncInitializer {
    private KeyValueService keyValueService;
    private Set<Schema> schemas;

    public static void createInitialTables(KeyValueService keyValueService, Set<Schema> schemas,
            boolean initializeAsync) {
        new TransactionManagersInitializer(keyValueService, schemas)
                .initialize(initializeAsync);
    }

    private TransactionManagersInitializer(KeyValueService keyValueService, Set<Schema> schemas) {
        this.keyValueService = keyValueService;
        this.schemas = schemas;
    }

    @Override
    @Idempotent
    public synchronized void tryInitialize() {
        TransactionTables.createTables(keyValueService);

        Set<Schema> allSchemas = ImmutableSet.<Schema>builder()
                .add(SweepSchema.INSTANCE.getLatestSchema())
                .addAll(schemas)
                .build();

        for (Schema schema : allSchemas) {
            Schemas.createTablesAndIndexes(schema, keyValueService);
        }

        // Prime the key value service with logging information.
        // TODO (jkong): Needs to be changed if/when we support dynamic table creation.
        LoggingArgs.hydrate(keyValueService.getMetadataForTables());
    }

    @Override
    protected String getInitializingClassName() {
        return "TransactionManagersInitializer";
    }
}
