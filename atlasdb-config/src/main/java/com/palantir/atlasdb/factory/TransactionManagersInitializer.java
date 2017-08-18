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
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableSet;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.schema.SweepSchema;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.impl.TransactionTables;

public class TransactionManagersInitializer implements AsyncInitializer {
    private static final AtomicBoolean isInitialized = new AtomicBoolean(false);

    private KeyValueService keyValueService;
    private Set<Schema> schemas;

    public TransactionManagersInitializer(KeyValueService keyValueService, Set<Schema> schemas) {
        this.keyValueService = keyValueService;
        this.schemas = schemas;
    }

    @Override
    public void cleanUpOnInitFailure() {

    }

    @Override
    public boolean isInitialized() {
        return isInitialized.get();
    }

    @Override
    public void tryInitialize() {
        TransactionTables.createTables(keyValueService);

        Set<Schema> allSchemas = ImmutableSet.<Schema>builder()
                .add(SweepSchema.INSTANCE.getLatestSchema())
                .addAll(schemas)
                .build();
        for (Schema schema : allSchemas) {
            Schemas.createTablesAndIndexes(schema, keyValueService);
        }

        LoggingArgs.hydrate(keyValueService.getMetadataForTables());

        if (!isInitialized.compareAndSet(false, true)) {
            log.warn("Someone initialized the class underneath us.");
        }
    }
}
