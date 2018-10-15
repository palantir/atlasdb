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
package com.palantir.atlasdb.schema;

import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.metadata.SchemaMetadataService;
import com.palantir.atlasdb.schema.metadata.SchemaMetadataServiceImpl;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.exception.NotInitializedException;

public class CleanupMetadataResourceImpl implements CleanupMetadataResource {
    private final Supplier<SchemaMetadataService> schemaMetadataServiceSupplier;

    @VisibleForTesting
    CleanupMetadataResourceImpl(Supplier<SchemaMetadataService> schemaMetadataServiceSupplier) {
        this.schemaMetadataServiceSupplier = schemaMetadataServiceSupplier;
    }

    public CleanupMetadataResourceImpl(TransactionManager transactionManager, boolean initializeAsync) {
        schemaMetadataServiceSupplier = Suppliers.memoize(() -> {
            if (!transactionManager.isInitialized()) {
                throw new NotInitializedException("CleanupMetadataResource");
            }
            return SchemaMetadataServiceImpl.create(transactionManager.getKeyValueService(), initializeAsync);
        });
    }

    @Override
    public Optional<SerializableCleanupMetadata> get(
            String schemaName,
            String tableName) {
        return schemaMetadataServiceSupplier.get()
                .loadSchemaMetadata(schemaName)
                .map(SchemaMetadata::schemaDependentTableMetadata)
                .flatMap(metadataMap -> Optional.ofNullable(metadataMap.get(
                        TableReference.createFromFullyQualifiedName(tableName))))
                .map(SchemaDependentTableMetadata::cleanupMetadata)
                .map(cleanupMetadata -> cleanupMetadata.accept(SerializableCleanupMetadata.SERIALIZER));
    }
}
