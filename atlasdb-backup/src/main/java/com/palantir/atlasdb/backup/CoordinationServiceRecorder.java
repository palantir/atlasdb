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
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadataState;
import com.palantir.atlasdb.internalschema.persistence.CoordinationServices;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Optional;
import java.util.function.Function;

final class CoordinationServiceRecorder {
    private static final SafeLogger log = SafeLoggerFactory.get(CoordinationServiceRecorder.class);

    private final Function<Namespace, KeyValueService> keyValueServiceFactory;
    private final BackupPersister backupPersister;

    CoordinationServiceRecorder(
            Function<Namespace, KeyValueService> keyValueServiceFactory, BackupPersister backupPersister) {
        this.keyValueServiceFactory = keyValueServiceFactory;
        this.backupPersister = backupPersister;
    }

    public void storeFastForwardState(CompletedBackup completedBackup) {
        Namespace namespace = completedBackup.getNamespace();
        Optional<InternalSchemaMetadataState> maybeMetadata =
                fetchSchemaMetadata(namespace, completedBackup.getBackupEndTimestamp());

        maybeMetadata.ifPresentOrElse(
                metadata -> backupPersister.storeSchemaMetadata(namespace, metadata),
                () -> logEmptyMetadata(namespace));
    }

    private void logEmptyMetadata(Namespace namespace) {
        log.info("No metadata stored in coordination service for namespace.", SafeArg.of("namespace", namespace));
    }

    private Optional<InternalSchemaMetadataState> fetchSchemaMetadata(Namespace namespace, long timestamp) {
        // TODO(gs): in ETE tests we DO NOT want to close the KVS, because the KVS is shared between tests
        //  probably should be fixed by creating a new KVS?
        //        try (KeyValueService kvs = keyValueServiceFactory.apply(namespace)) {
        KeyValueService kvs = keyValueServiceFactory.apply(namespace);
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
