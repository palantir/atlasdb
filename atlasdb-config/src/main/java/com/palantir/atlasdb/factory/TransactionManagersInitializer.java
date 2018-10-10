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

import static java.util.stream.Collectors.toSet;

import java.util.Collection;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.schema.metadata.SchemaMetadataService;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.common.annotation.Idempotent;

public final class TransactionManagersInitializer extends AsyncInitializer {

    private static final Logger log = LoggerFactory.getLogger(TransactionManagersInitializer.class);

    private KeyValueService keyValueService;
    private Set<Schema> schemas;
    private SchemaMetadataService schemaMetadataService;

    public static TransactionManagersInitializer createInitialTables(
            KeyValueService keyValueService,
            Set<Schema> schemas,
            SchemaMetadataService schemaMetadataService,
            boolean initializeAsync) {
        TransactionManagersInitializer initializer = new TransactionManagersInitializer(
                keyValueService, schemas, schemaMetadataService);
        initializer.initialize(initializeAsync);
        return initializer;
    }

    @VisibleForTesting
    TransactionManagersInitializer(
            KeyValueService keyValueService,
            Set<Schema> schemas,
            SchemaMetadataService schemaMetadataService) {
        this.keyValueService = keyValueService;
        this.schemas = schemas;
        this.schemaMetadataService = schemaMetadataService;
    }

    @Override
    @Idempotent
    public synchronized void tryInitialize() {
        TransactionTables.createTables(keyValueService);

        createTablesAndIndexes();
        deleteDeprecatedTablesAndIndexes();
        populateLoggingContext();
    }

    private void createTablesAndIndexes() {
        for (Schema schema : schemas) {
            Schemas.createTablesAndIndexes(schema, keyValueService);
            schemaMetadataService.putSchemaMetadata(schema.getName(), schema.getSchemaMetadata());
        }
    }

    private void deleteDeprecatedTablesAndIndexes() {
        schemas.forEach(Schema::validate);
        Set<TableReference> allDeprecatedTables = schemas.stream()
                .map(Schema::getDeprecatedTables)
                .flatMap(Collection::stream)
                .collect(toSet());

        try {
            keyValueService.dropTables(allDeprecatedTables);
        } catch (InsufficientConsistencyException e) {
            log.info("Could not drop deprecated tables due to insufficient consistency from the underlying "
                    + "KeyValueService.");
        }
    }

    private void populateLoggingContext() {
        // TODO (jkong): Needs to be changed if/when we support dynamic table creation.
        LoggingArgs.hydrate(keyValueService.getMetadataForTables());
    }

    @Override
    protected String getInitializingClassName() {
        return "TransactionManagersInitializer";
    }
}
