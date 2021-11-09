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

import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadataState;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class InMemorySchemaMetadataPersister implements SchemaMetadataPersister {
    private static final SafeLogger log = SafeLoggerFactory.get(SchemaMetadataPersister.class);

    // I think we'll only need to _persist_ for in progress.
    // We can receive and check for fast forward together
    private final Map<Namespace, InternalSchemaMetadataState> metadataForInProgressBackups;

    public InMemorySchemaMetadataPersister() {
        metadataForInProgressBackups = new ConcurrentHashMap<>();
    }

    @Override
    public void persistAtBackupTimestamp(Namespace namespace, InternalSchemaMetadataState internalSchemaMetadataState) {
        metadataForInProgressBackups.put(namespace, internalSchemaMetadataState);
    }

    @Override
    public boolean inProgressMetadataExists(Namespace namespace) {
        return metadataForInProgressBackups.containsKey(namespace);
    }

    @Override
    public boolean verifyFastForwardState(
            Namespace namespace, Optional<InternalSchemaMetadataState> fastForwardState, Long backupTimestamp) {
        Optional<InternalSchemaMetadataState> maybeBackupTsMetadata =
                Optional.ofNullable(metadataForInProgressBackups.remove(namespace));

        if (maybeBackupTsMetadata.equals(fastForwardState)) {
            // States equal - don't need to compare
            return true;
        }

        Optional<Long> maybeBackupBound = maybeBackupTsMetadata
                .flatMap(InternalSchemaMetadataState::value)
                .map(ValueAndBound::bound);

        if (maybeBackupBound.isPresent() && backupTimestamp <= maybeBackupBound.get()) {
            return true;
        }

        log.warn(
                "The coordination service is in a state where we cannot guarantee a consistent backup. This state"
                        + " is valid, but expected to occur very infrequently. One can retry the backup without other"
                        + " manual intervention.",
                SafeArg.of("namespace", namespace.get()),
                SafeArg.of("backupInternalSchemaMetadataState", maybeBackupTsMetadata),
                SafeArg.of("fastForwardInternalSchemaMetadataState", fastForwardState),
                SafeArg.of("backupTimestamp", backupTimestamp));
        return false;
    }
}
