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
package com.palantir.atlasdb.factory;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.cell.api.DataKeyValueServiceManager;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.common.annotation.Idempotent;
import java.util.Set;

public final class TransactionManagersInitializer extends AsyncInitializer {

    private KeyValueService internalKeyValueService;

    private DataKeyValueServiceManager dataKeyValueServiceManager;
    private Set<Schema> schemas;
    private final boolean allSafeForLogging;

    public static TransactionManagersInitializer createInitialTables(
            KeyValueService internalKeyValueService,
            DataKeyValueServiceManager dataKeyValueServiceManager,
            Set<Schema> schemas,
            boolean initializeAsync,
            boolean allSafeForLogging) {
        TransactionManagersInitializer initializer = new TransactionManagersInitializer(
                internalKeyValueService, dataKeyValueServiceManager, schemas, allSafeForLogging);
        initializer.initialize(initializeAsync);
        return initializer;
    }

    @VisibleForTesting
    TransactionManagersInitializer(
            KeyValueService internalKeyValueService,
            DataKeyValueServiceManager dataKeyValueServiceManager,
            Set<Schema> schemas,
            boolean allSafeForLogging) {
        this.internalKeyValueService = internalKeyValueService;
        this.dataKeyValueServiceManager = dataKeyValueServiceManager;
        this.schemas = schemas;
        this.allSafeForLogging = allSafeForLogging;
    }

    @Override
    @Idempotent
    public synchronized void tryInitialize() {
        TransactionTables.createTables(internalKeyValueService);

        createTablesAndIndexes();
        populateLoggingContext();
    }

    private void createTablesAndIndexes() {
        for (Schema schema : schemas) {
            Schemas.createUserTablesAndIndexes(schema, dataKeyValueServiceManager.getDdlManager());
        }
    }

    private void populateLoggingContext() {
        // TODO (jkong): Needs to be changed if/when we support dynamic table creation.
        LoggingArgs.combineAndSetNewAllSafeForLoggingFlag(allSafeForLogging);
        if (!allSafeForLogging) {
            LoggingArgs.hydrate(internalKeyValueService.getMetadataForTables());
        }
    }

    @Override
    protected String getInitializingClassName() {
        return "TransactionManagersInitializer";
    }
}
