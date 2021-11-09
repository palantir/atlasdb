/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.backup;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadataState;
import com.palantir.atlasdb.internalschema.persistence.CoordinationServices;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;
import com.palantir.atlasdb.timelock.api.CompletedBackup;
import com.palantir.atlasdb.timelock.api.Namespace;
import java.util.Optional;
import java.util.function.LongSupplier;

public class CoordinationServiceRecorder {
    private final Namespace namespace;
    private final KeyValueService keyValueService;
    private final LongSupplier timestampSupplier;
    private final SchemaMetadataPersister schemaMetadataPersister;

    public CoordinationServiceRecorder(
            Namespace namespace,
            KeyValueService keyValueService,
            LongSupplier timestampSupplier,
            SchemaMetadataPersister schemaMetadataPersister) {
        this.namespace = namespace;
        this.keyValueService = keyValueService;
        this.timestampSupplier = timestampSupplier;
        this.schemaMetadataPersister = schemaMetadataPersister;
    }

    // TODO(gs): do away with TypedTimestamp?
    public void recordAtBackupTimestamp(TypedTimestamp timestamp) {
        fetchSchemaMetadata(timestamp)
                .ifPresent(metadata -> schemaMetadataPersister.persistAtBackupTimestamp(namespace, metadata));
    }

    public boolean verifyFastForwardState(CompletedBackup completedBackup) {
        TypedTimestamp fastForwardTimestamp =
                TypedTimestamp.of(TimestampType.FAST_FORWARD, completedBackup.getBackupEndTimestamp());
        Optional<InternalSchemaMetadataState> fastForwardMetadata = fetchSchemaMetadata(fastForwardTimestamp);

        // TODO(gs): is this the right ts?
        schemaMetadataPersister.verifyFastForwardState(
                namespace, fastForwardMetadata, fastForwardTimestamp.timestamp());
        return true;
    }

    private Optional<InternalSchemaMetadataState> fetchSchemaMetadata(TypedTimestamp timestamp) {
        if (!keyValueService.getAllTableNames().contains(AtlasDbConstants.COORDINATION_TABLE)) {
            return Optional.empty();
        }
        CoordinationService<InternalSchemaMetadata> coordination =
                CoordinationServices.createDefault(keyValueService, timestampSupplier, false);

        return Optional.of(InternalSchemaMetadataState.of(getValidMetadata(timestamp.timestamp(), coordination)));
    }

    private ValueAndBound<InternalSchemaMetadata> getValidMetadata(
            long timestamp, CoordinationService<InternalSchemaMetadata> coordination) {
        Optional<ValueAndBound<InternalSchemaMetadata>> state = coordination.getValueForTimestamp(timestamp);
        while (state.isEmpty()) {
            state = tryExtendValidityBound(coordination, timestamp);
        }
        return state.get();
    }

    private Optional<ValueAndBound<InternalSchemaMetadata>> tryExtendValidityBound(
            CoordinationService<InternalSchemaMetadata> coordination, long timestamp) {
        CheckAndSetResult<ValueAndBound<InternalSchemaMetadata>> casResult = coordination.tryTransformCurrentValue(
                valueAndBound -> valueAndBound.value().orElseGet(InternalSchemaMetadata::defaultValue));
        ValueAndBound<InternalSchemaMetadata> persisted = Iterables.getOnlyElement(casResult.existingValues());
        return (persisted.bound() >= timestamp) ? Optional.of(persisted) : Optional.empty();
    }
}
