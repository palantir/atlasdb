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

import com.palantir.atlasdb.backup.api.AtlasService;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.backup.api.InProgressBackupToken;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadataState;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

class InMemoryBackupPersister implements BackupPersister {
    private final Map<AtlasService, Long> immutableTimestamps;
    private final Map<AtlasService, InternalSchemaMetadataState> schemaMetadatas;
    private final Map<AtlasService, CompletedBackup> completedBackups;

    InMemoryBackupPersister() {
        schemaMetadatas = new ConcurrentHashMap<>();
        completedBackups = new ConcurrentHashMap<>();
        immutableTimestamps = new ConcurrentHashMap<>();
    }

    @Override
    public void storeSchemaMetadata(
            AtlasService atlasService, InternalSchemaMetadataState internalSchemaMetadataState) {
        schemaMetadatas.put(atlasService, internalSchemaMetadataState);
    }

    @Override
    public Optional<InternalSchemaMetadataState> getSchemaMetadata(AtlasService atlasService) {
        return Optional.ofNullable(schemaMetadatas.get(atlasService));
    }

    @Override
    public void storeCompletedBackup(AtlasService atlasService, CompletedBackup completedBackup) {
        completedBackups.put(atlasService, completedBackup);
    }

    @Override
    public Optional<CompletedBackup> getCompletedBackup(AtlasService atlasService) {
        return Optional.ofNullable(completedBackups.get(atlasService));
    }

    @Override
    public void storeImmutableTimestamp(AtlasService atlasService, InProgressBackupToken inProgressBackupToken) {
        immutableTimestamps.put(atlasService, inProgressBackupToken.getImmutableTimestamp());
    }

    @Override
    public Optional<Long> getImmutableTimestamp(AtlasService atlasService) {
        return Optional.ofNullable(immutableTimestamps.get(atlasService));
    }
}
