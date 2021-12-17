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

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.cassandra.backup.CassandraRepairHelper;
import com.palantir.atlasdb.cassandra.backup.RangesForRepair;
import com.palantir.atlasdb.timelock.api.Namespace;
import java.util.function.BiConsumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AtlasRestoreServiceTest {
    private static final Namespace WITH_BACKUP = Namespace.of("with-backup");
    private static final Namespace NO_BACKUP = Namespace.of("no-backup");

    @Mock
    private CassandraRepairHelper cassandraRepairHelper;

    private AtlasRestoreService atlasRestoreService;

    @Before
    public void setup() {
        BackupPersister backupPersister = new InMemoryBackupPersister();
        atlasRestoreService = new AtlasRestoreService(backupPersister, cassandraRepairHelper);

        CompletedBackup completedBackup = CompletedBackup.builder()
                .namespace(WITH_BACKUP)
                .immutableTimestamp(1L)
                .backupStartTimestamp(2L)
                .backupEndTimestamp(3L)
                .build();
        backupPersister.storeCompletedBackup(completedBackup);
    }

    @Test
    public void repairsOnlyWhenBackupPresent() {
        BiConsumer<String, RangesForRepair> doNothingConsumer = (_unused1, _unused2) -> {};
        atlasRestoreService.repairInternalTables(ImmutableSet.of(WITH_BACKUP, NO_BACKUP), doNothingConsumer);

        verify(cassandraRepairHelper).repairInternalTables(WITH_BACKUP, doNothingConsumer);
        verify(cassandraRepairHelper).repairTransactionsTables(eq(WITH_BACKUP), anyMap(), eq(doNothingConsumer));
        verifyNoMoreInteractions(cassandraRepairHelper);
    }
}
