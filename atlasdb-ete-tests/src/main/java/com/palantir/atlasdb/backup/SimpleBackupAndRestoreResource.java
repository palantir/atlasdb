/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.timelock.api.Namespace;
import java.util.Optional;
import java.util.Set;

public class SimpleBackupAndRestoreResource implements BackupAndRestoreResource {
    private final AtlasBackupService atlasBackupService;
    private final AtlasRestoreService atlasRestoreService;
    private final ExternalBackupPersister externalBackupPersister;

    public SimpleBackupAndRestoreResource(
            AtlasBackupService atlasBackupService,
            AtlasRestoreService atlasRestoreService,
            ExternalBackupPersister externalBackupPersister) {
        this.atlasBackupService = atlasBackupService;
        this.atlasRestoreService = atlasRestoreService;
        this.externalBackupPersister = externalBackupPersister;
    }

    @Override
    public Set<Namespace> prepareBackup(Set<Namespace> namespaces) {
        return atlasBackupService.prepareBackup(namespaces);
    }

    @Override
    public Set<Namespace> completeBackup(Set<Namespace> namespaces) {
        return atlasBackupService.completeBackup(namespaces);
    }

    @Override
    public Set<Namespace> prepareRestore(UniqueBackup uniqueBackup) {
        return atlasRestoreService.prepareRestore(uniqueBackup.namespaces(), uniqueBackup.backupId());
    }

    @Override
    public Set<Namespace> completeRestore(UniqueBackup uniqueBackup) {
        return atlasRestoreService.completeRestore(uniqueBackup.namespaces(), uniqueBackup.backupId());
    }

    @Override
    public Optional<Long> getStoredImmutableTimestamp(Namespace namespace) {
        return externalBackupPersister.getImmutableTimestamp(namespace);
    }

    @Override
    public Optional<CompletedBackup> getStoredBackup(Namespace namespace) {
        return externalBackupPersister.getCompletedBackup(namespace);
    }
}
