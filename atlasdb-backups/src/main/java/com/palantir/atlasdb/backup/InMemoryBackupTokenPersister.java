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

import com.palantir.atlasdb.timelock.api.BackupToken;
import com.palantir.atlasdb.timelock.api.Namespace;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class InMemoryBackupTokenPersister implements BackupTokenPersister {
    private final Map<Namespace, BackupToken> storedTokens;

    public InMemoryBackupTokenPersister() {
        storedTokens = new HashMap<>();
    }

    @Override
    public boolean storeBackupToken(BackupToken backupToken) {
        storedTokens.put(backupToken.getNamespace(), backupToken);
        return true;
    }

    @Override
    public Optional<BackupToken> retrieveBackupToken(Namespace namespace) {
        return Optional.ofNullable(storedTokens.get(namespace));
    }
}
