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

import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.tokens.auth.AuthHeader;
import java.util.Optional;
import java.util.Set;

public class SimpleBackupAndRestoreResource implements BackupAndRestoreResource {
    private final AtlasBackupService atlasBackupService;
    private final ExternalBackupPersister externalBackupPersister;

    public SimpleBackupAndRestoreResource(
            AtlasBackupService atlasBackupService, ExternalBackupPersister externalBackupPersister) {
        this.atlasBackupService = atlasBackupService;
        this.externalBackupPersister = externalBackupPersister;
    }

    @Override
    public Set<Namespace> prepareBackup(AuthHeader authHeader, Set<Namespace> namespaces) {
        return atlasBackupService.prepareBackup(namespaces);
    }

    @Override
    public Optional<Long> getStoredImmutableTimestamp(Namespace namespace) {
        return externalBackupPersister.getImmutableTimestamp(namespace);
    }
}
