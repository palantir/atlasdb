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

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.backup.api.AtlasService;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.backup.api.InProgressBackupToken;
import com.palantir.atlasdb.backup.api.ServiceId;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadataState;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.lock.v2.LockToken;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ExternalBackupPersisterTest {
    private static final Namespace NAMESPACE = Namespace.of("broken_namespace");
    private static final AtlasService ATLAS_SERVICE = AtlasService.of(ServiceId.of("service"), NAMESPACE);

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private ExternalBackupPersister externalBackupPersister;

    @Before
    public void setUp() {
        externalBackupPersister = new ExternalBackupPersister(this::getPath);
    }

    @Test
    public void getSchemaMetadataWhenEmpty() {
        assertThat(externalBackupPersister.getSchemaMetadata(ATLAS_SERVICE)).isEmpty();
    }

    @Test
    public void putAndGetSchemaMetadata() {
        InternalSchemaMetadata internalSchemaMetadata = InternalSchemaMetadata.defaultValue();
        InternalSchemaMetadataState state =
                InternalSchemaMetadataState.of(ValueAndBound.of(internalSchemaMetadata, 100L));
        externalBackupPersister.storeSchemaMetadata(ATLAS_SERVICE, state);

        assertThat(externalBackupPersister.getSchemaMetadata(ATLAS_SERVICE)).contains(state);
    }

    @Test
    public void getImmutableTimestampWhenEmpty() {
        assertThat(externalBackupPersister.getImmutableTimestamp(ATLAS_SERVICE)).isEmpty();
    }

    @Test
    public void putAndGetImmutableTimestamp() {
        long immutableTimestamp = 1337L;
        InProgressBackupToken inProgressBackupToken = InProgressBackupToken.builder()
                .namespace(NAMESPACE)
                .immutableTimestamp(immutableTimestamp)
                .backupStartTimestamp(3141L)
                .lockToken(LockToken.of(UUID.randomUUID()))
                .build();
        externalBackupPersister.storeImmutableTimestamp(ATLAS_SERVICE, inProgressBackupToken);
        assertThat(externalBackupPersister.getImmutableTimestamp(ATLAS_SERVICE)).contains(immutableTimestamp);
    }

    @Test
    public void getCompletedBackupWhenEmpty() {
        assertThat(externalBackupPersister.getCompletedBackup(ATLAS_SERVICE)).isEmpty();
    }

    @Test
    public void putAndGetCompletedBackup() {
        CompletedBackup completedBackup = CompletedBackup.builder()
                .namespace(NAMESPACE)
                .immutableTimestamp(1L)
                .backupStartTimestamp(2L)
                .backupEndTimestamp(3L)
                .build();
        externalBackupPersister.storeCompletedBackup(ATLAS_SERVICE, completedBackup);

        assertThat(externalBackupPersister.getCompletedBackup(ATLAS_SERVICE)).contains(completedBackup);
    }

    private Path getPath(AtlasService atlasService) {
        try {
            return getOrCreateFolder(atlasService).toPath();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private File getOrCreateFolder(AtlasService atlasService) throws IOException {
        File file = new File(tempFolder.getRoot(), atlasService.getNamespace().value());
        return file.exists()
                ? file
                : tempFolder.newFolder(atlasService.getNamespace().value());
    }
}
