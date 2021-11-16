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

import com.palantir.atlasdb.backup.api.BackupId;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadataState;
import com.palantir.atlasdb.timelock.api.Namespace;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ExternalBackupPersisterTest {
    private static final BackupId BACKUP_ID = BackupId.of("back_when_stuff_worked");
    private static final Namespace NAMESPACE = Namespace.of("broken_namespace");

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private ExternalBackupPersister externalBackupPersister;

    @Before
    public void setUp() {
        externalBackupPersister = new ExternalBackupPersister(this::getPath);
    }

    @Test
    public void getSchemaMetadataWhenEmpty() {
        assertThat(externalBackupPersister.getSchemaMetadata(BACKUP_ID, NAMESPACE))
                .isEmpty();
    }

    @Test
    public void putAndGetSchemaMetadata() {
        InternalSchemaMetadata internalSchemaMetadata = InternalSchemaMetadata.defaultValue();
        InternalSchemaMetadataState state =
                InternalSchemaMetadataState.of(ValueAndBound.of(internalSchemaMetadata, 100L));
        externalBackupPersister.storeSchemaMetadata(BACKUP_ID, NAMESPACE, state);

        assertThat(externalBackupPersister.getSchemaMetadata(BACKUP_ID, NAMESPACE))
                .contains(state);
    }

    @Test
    public void getCompletedBackupWhenEmpty() {
        assertThat(externalBackupPersister.getCompletedBackup(BACKUP_ID, NAMESPACE))
                .isEmpty();
    }

    @Test
    public void putAndGetCompletedBackup() {
        CompletedBackup completedBackup = CompletedBackup.builder()
                .backupId(BACKUP_ID)
                .namespace(NAMESPACE)
                .backupStartTimestamp(1L)
                .backupEndTimestamp(2L)
                .build();
        externalBackupPersister.storeCompletedBackup(completedBackup);

        assertThat(externalBackupPersister.getCompletedBackup(BACKUP_ID, NAMESPACE))
                .contains(completedBackup);
    }

    private Path getPath(BackupId backupId, Namespace namespace) {
        try {
            return tempFolder.newFolder(backupId.get(), namespace.get()).toPath();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
