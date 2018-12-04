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

package com.palantir.atlasdb.internalschema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.exception.NotInitializedException;

/**
 * Writes an initial version of the {@link InternalSchemaMetadata} to the database.
 */
public final class InternalSchemaMetadataInitializer extends AsyncInitializer {
    private static final Logger log = LoggerFactory.getLogger(InternalSchemaMetadataInitializer.class);

    private static final long START_OF_TIME = 1L;
    private static final Range<Long> ALL_TIMESTAMPS = Range.atLeast(START_OF_TIME);

    private final CoordinationService<InternalSchemaMetadata> coordinationService;

    private InternalSchemaMetadataInitializer(
            CoordinationService<InternalSchemaMetadata> coordinationService) {
        this.coordinationService = coordinationService;
    }

    public static InternalSchemaMetadataInitializer createAndInitialize(
            CoordinationService<InternalSchemaMetadata> coordinationService,
            boolean initializeAsync) {
        InternalSchemaMetadataInitializer initializer = new InternalSchemaMetadataInitializer(coordinationService);
        initializer.initialize(initializeAsync);
        return initializer;
    }

    @Override
    protected void tryInitialize() {
        boolean metadataInitialized = attemptToInitializeSchemaMetadata();
        if (!metadataInitialized) {
            throw new NotInitializedException("The schema metadata service wasn't actually initialized");
        }
    }

    /**
     * Once this method returns true, it can be guaranteed that the {@link CoordinationService} associated with this
     * {@link InternalSchemaMetadataInitializer} will contain a value. This may not necessarily be the initial
     * value.
     */
    private boolean attemptToInitializeSchemaMetadata() {
        if (valueKnownToExist()) {
            return true;
        }

        CheckAndSetResult<ValueAndBound<InternalSchemaMetadata>> checkAndSetResult
                = coordinationService.tryTransformCurrentValue(valueAndBound -> valueAndBound.value()
                .orElseGet(this::getDefaultInternalSchemaMetadata));

        return checkAndSetResult.successful() || checkAndSetResult.existingValues().size() > 0;
    }

    private boolean valueKnownToExist() {
        try {
            return coordinationService.getValueForTimestamp(START_OF_TIME).isPresent();
        } catch (Exception e) {
            log.warn("Exception occurred when trying to initialize schema metadata.", e);
            return false;
        }
    }

    private InternalSchemaMetadata getDefaultInternalSchemaMetadata() {
        return InternalSchemaMetadata.builder()
                .timestampToTransactionsTableSchemaVersion(
                        TimestampPartitioningMap.of(ImmutableRangeMap.of(
                                ALL_TIMESTAMPS, TransactionConstants.TRANSACTIONS_TABLE_SCHEMA_VERSION)))
                .build();
    }

    @Override
    protected String getInitializingClassName() {
        return InternalSchemaMetadataInitializer.class.getSimpleName();
    }
}
