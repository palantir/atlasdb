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

package com.palantir.atlasdb.schema.metadata;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.SchemaMetadata;
import com.palantir.logsafe.UnsafeArg;

public final class SchemaMetadataServiceImpl implements SchemaMetadataService {
    @VisibleForTesting
    class InitializingWrapper extends AsyncInitializer implements AutoDelegate_SchemaMetadataService {
        @Override
        public SchemaMetadataService delegate() {
            checkInitialized();
            return SchemaMetadataServiceImpl.this;
        }

        @Override
        protected void tryInitialize() {
            SchemaMetadataServiceImpl.this.tryInitialize();
        }

        @Override
        protected String getInitializingClassName() {
            return SchemaMetadataService.class.getSimpleName();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(SchemaMetadataServiceImpl.class);

    private static final long QUERY_TIMESTAMP = 1L;
    private static final byte[] COLUMN_NAME = PtBytes.toBytes("m");

    private final KeyValueService keyValueService;
    private final InitializingWrapper wrapper = new InitializingWrapper();

    private SchemaMetadataServiceImpl(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
    }

    public static SchemaMetadataService create(KeyValueService keyValueService, boolean initializeAsync) {
        SchemaMetadataServiceImpl metadataService = new SchemaMetadataServiceImpl(keyValueService);
        metadataService.wrapper.initialize(initializeAsync);
        return metadataService.wrapper.isInitialized() ? metadataService : metadataService.wrapper;
    }

    protected void initialize(boolean asyncInitialize) {
        wrapper.initialize(asyncInitialize);
    }

    private void tryInitialize() {
        keyValueService.createTable(
                AtlasDbConstants.DEFAULT_SCHEMA_METADATA_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    @Override
    public Optional<SchemaMetadata> loadSchemaMetadata(String schemaName) {
        return loadMetadataCellFromKeyValueService(schemaName)
                .map(SchemaMetadata.HYDRATOR::hydrateFromBytes);
    }

    @Override
    public void putSchemaMetadata(String schemaName, SchemaMetadata schemaMetadata) {
        byte[] serializedMetadata = schemaMetadata.persistToBytes();
        while (!Thread.currentThread().isInterrupted()) {
            Optional<byte[]> existingMetadata = loadMetadataCellFromKeyValueService(schemaName);
            CheckAndSetRequest request = existingMetadata.map(existingData -> CheckAndSetRequest.singleCell(
                    AtlasDbConstants.DEFAULT_SCHEMA_METADATA_TABLE,
                    createCellForGivenSchemaName(schemaName),
                    existingData,
                    serializedMetadata))
                    .orElse(CheckAndSetRequest.newCell(
                            AtlasDbConstants.DEFAULT_SCHEMA_METADATA_TABLE,
                            createCellForGivenSchemaName(schemaName),
                            serializedMetadata));
            try {
                keyValueService.checkAndSet(request);
                log.info("Successfully updated the schema metadata to {}.",
                        UnsafeArg.of("committedUpdate", request));
                return;
            } catch (CheckAndSetException e) {
                log.info("Failed to update the schema metadata: {}. Retrying.",
                        UnsafeArg.of("attemptedUpdate", request));
            }
        }
    }

    @Override
    public Map<String, SchemaMetadata> getAllSchemaMetadata() {
        // RangeRequest here is ok as the table is small
        return keyValueService.getRange(AtlasDbConstants.DEFAULT_SCHEMA_METADATA_TABLE,
                RangeRequest.all(),
                QUERY_TIMESTAMP).stream()
                .collect(Collectors.toMap(
                        result -> PtBytes.toString(result.getRowName()),
                        result -> SchemaMetadata.HYDRATOR.hydrateFromBytes(result.getOnlyColumnValue().getContents())
                ));
    }

    @Override
    public void deleteSchemaMetadata(String schemaName) {
        keyValueService.delete(AtlasDbConstants.DEFAULT_SCHEMA_METADATA_TABLE,
                ImmutableMultimap.of(createCellForGivenSchemaName(schemaName), 0L));
    }

    @Override
    public boolean isInitialized() {
        return keyValueService.isInitialized();
    }

    private Cell createCellForGivenSchemaName(String schemaName) {
        return Cell.create(PtBytes.toBytes(schemaName), COLUMN_NAME);
    }

    private Optional<byte[]> loadMetadataCellFromKeyValueService(String schemaName) {
        Map<Cell, Value> kvsData = keyValueService.get(AtlasDbConstants.DEFAULT_SCHEMA_METADATA_TABLE,
                ImmutableMap.of(createCellForGivenSchemaName(schemaName), QUERY_TIMESTAMP));
        return kvsData.isEmpty()
                ? Optional.empty()
                : Optional.of(Iterables.getOnlyElement(kvsData.values()).getContents());
    }
}
