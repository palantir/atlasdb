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
import com.palantir.atlasdb.backup.api.AtlasService;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadataState;
import com.palantir.atlasdb.internalschema.persistence.CoordinationServices;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Optional;

final class CoordinationServiceRecorder {
    private static final SafeLogger log = SafeLoggerFactory.get(CoordinationServiceRecorder.class);

    private final KvsRunner kvsRunner;
    private final BackupPersister backupPersister;

    CoordinationServiceRecorder(KvsRunner kvsRunner, BackupPersister backupPersister) {
        this.kvsRunner = kvsRunner;
        this.backupPersister = backupPersister;
    }

    public void storeFastForwardState(CompletedBackup completedBackup) {
        AtlasService atlasService = completedBackup.getAtlasService();
        Optional<InternalSchemaMetadataState> maybeMetadata =
                fetchSchemaMetadata(atlasService, completedBackup.getBackupEndTimestamp());

        maybeMetadata.ifPresentOrElse(
                metadata -> backupPersister.storeSchemaMetadata(atlasService, metadata),
                () -> logEmptyMetadata(atlasService));
    }

    private void logEmptyMetadata(AtlasService atlasService) {
        log.info(
                "No metadata stored in coordination service for atlas service.",
                SafeArg.of("atlasService", atlasService));
    }

    private Optional<InternalSchemaMetadataState> fetchSchemaMetadata(AtlasService atlasService, long timestamp) {
        return kvsRunner.run(atlasService, kvs -> getInternalSchemaMetadataState(kvs, timestamp));
    }

    private Optional<InternalSchemaMetadataState> getInternalSchemaMetadataState(KeyValueService kvs, long timestamp) {
        if (!kvs.getAllTableNames().contains(AtlasDbConstants.COORDINATION_TABLE)) {
            return Optional.empty();
        }
        CoordinationService<InternalSchemaMetadata> coordination =
                CoordinationServices.createDefault(kvs, () -> timestamp, false);

        return Optional.of(InternalSchemaMetadataState.of(getValidMetadata(coordination, timestamp)));
    }

    private ValueAndBound<InternalSchemaMetadata> getValidMetadata(
            CoordinationService<InternalSchemaMetadata> coordination, long timestamp) {
        return coordination
                .getValueForTimestamp(timestamp)
                .orElseGet(() -> extendValidityBound(coordination, timestamp));
    }

    // Keep trying to extend the validity bound until it succeeds. Transactions can't commit until _someone_ succeeds
    // in doing this - so this is effectively guaranteed to terminate eventually.
    private ValueAndBound<InternalSchemaMetadata> extendValidityBound(
            CoordinationService<InternalSchemaMetadata> coordination, long timestamp) {
        Optional<ValueAndBound<InternalSchemaMetadata>> maybeBound = Optional.empty();
        while (maybeBound.isEmpty()) {
            maybeBound = tryExtendValidityBound(coordination, timestamp);
        }
        return maybeBound.get();
    }

    private Optional<ValueAndBound<InternalSchemaMetadata>> tryExtendValidityBound(
            CoordinationService<InternalSchemaMetadata> coordination, long timestamp) {
        CheckAndSetResult<ValueAndBound<InternalSchemaMetadata>> casResult = coordination.tryTransformCurrentValue(
                valueAndBound -> valueAndBound.value().orElseGet(InternalSchemaMetadata::defaultValue));
        ValueAndBound<InternalSchemaMetadata> persisted = Iterables.getOnlyElement(casResult.existingValues());
        return (persisted.bound() >= timestamp) ? Optional.of(persisted) : Optional.empty();
    }
}
