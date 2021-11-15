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

import com.palantir.atlasdb.internalschema.InternalSchemaMetadataState;
import com.palantir.atlasdb.timelock.api.CompletedBackup;
import com.palantir.atlasdb.timelock.api.Namespace;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

class InMemoryBackupPersister implements BackupPersister {
    private final Map<Namespace, InternalSchemaMetadataState> schemaMetadatas;
    private final Map<Namespace, CompletedBackup> completedBackups;

    InMemoryBackupPersister() {
        schemaMetadatas = new ConcurrentHashMap<>();
        completedBackups = new ConcurrentHashMap<>();
    }

    @Override
    public void storeSchemaMetadata(Namespace namespace, InternalSchemaMetadataState internalSchemaMetadataState) {
        schemaMetadatas.put(namespace, internalSchemaMetadataState);
    }

    @Override
    public Optional<InternalSchemaMetadataState> getSchemaMetadata(Namespace namespace) {
        return Optional.ofNullable(schemaMetadatas.get(namespace));
    }

    @Override
    public void storeCompletedBackup(CompletedBackup completedBackup) {
        completedBackups.put(completedBackup.getNamespace(), completedBackup);
    }

    // TODO(gs): store for multiple backups?
    @Override
    public Optional<CompletedBackup> getCompletedBackup(Namespace namespace, CompletedBackup completedBackup) {
        return Optional.ofNullable(completedBackups.get(namespace));
    }
}
