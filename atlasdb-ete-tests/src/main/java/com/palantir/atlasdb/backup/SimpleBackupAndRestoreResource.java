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

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.backup.api.AtlasService;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
    public Set<AtlasService> prepareBackup(Set<AtlasService> atlasServices) {
        return atlasBackupService.prepareBackup(atlasServices);
    }

    @Override
    public Set<AtlasService> completeBackup(Set<AtlasService> atlasServices) {
        return atlasBackupService.completeBackup(atlasServices);
    }

    @Override
    public Set<AtlasService> prepareRestore(Set<RestoreRequestWithId> requests) {
        // Would be more efficient to group by backup ID, but this is fine for ETE purposes
        return requests.stream()
                .map(request -> atlasRestoreService.prepareRestore(
                        ImmutableSet.of(request.restoreRequest()), request.backupId()))
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<AtlasService> completeRestore(Set<RestoreRequestWithId> requests) {
        return requests.stream()
                .map(request -> atlasRestoreService.completeRestore(
                        ImmutableSet.of(request.restoreRequest()), request.backupId()))
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    @Override
    public Optional<Long> getStoredImmutableTimestamp(AtlasService atlasService) {
        return externalBackupPersister.getImmutableTimestamp(atlasService);
    }

    @Override
    public Optional<CompletedBackup> getStoredBackup(AtlasService atlasService) {
        return externalBackupPersister.getCompletedBackup(atlasService);
    }
}
