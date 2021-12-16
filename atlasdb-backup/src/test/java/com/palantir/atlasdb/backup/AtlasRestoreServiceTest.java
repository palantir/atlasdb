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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.palantir.atlasdb.backup.api.AtlasRestoreClientBlocking;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.cassandra.backup.CassandraRepairHelper;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.tokens.auth.AuthHeader;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
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
    private AuthHeader authHeader;

    @Mock
    private AtlasRestoreClientBlocking atlasRestoreClient;

    @Mock
    private CassandraRepairHelper cassandraRepairHelper;

    private AtlasRestoreService atlasRestoreService;

    @Before
    public void setup() {
        BackupPersister backupPersister = new InMemoryBackupPersister();
        atlasRestoreService =
                new AtlasRestoreService(authHeader, atlasRestoreClient, backupPersister, cassandraRepairHelper);

        CompletedBackup completedBackup = CompletedBackup.builder()
                .namespace(WITH_BACKUP)
                .backupStartTimestamp(1L)
                .backupEndTimestamp(2L)
                .build();
        backupPersister.storeCompletedBackup(completedBackup);
    }

    @Test
    public void repairsOnlyWhenBackupPresent() {
        Consumer<Map<InetSocketAddress, Set<Range<LightweightOppToken>>>> doNothingConsumer = _unused -> {};
        atlasRestoreService.repairInternalTables(ImmutableSet.of(WITH_BACKUP, NO_BACKUP), doNothingConsumer);

        verify(cassandraRepairHelper).repairInternalTables(WITH_BACKUP, doNothingConsumer);
        verifyNoMoreInteractions(cassandraRepairHelper);
    }
}
