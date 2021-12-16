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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.palantir.atlasdb.backup.api.AtlasRestoreClientBlocking;
import com.palantir.atlasdb.backup.api.CompleteRestoreRequest;
import com.palantir.atlasdb.backup.api.CompleteRestoreResponse;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.cassandra.backup.CassandraRepairHelper;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.tokens.auth.AuthHeader;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AtlasRestoreServiceTest {
    private static final Namespace WITH_BACKUP = Namespace.of("with-backup");
    private static final Namespace NO_BACKUP = Namespace.of("no-backup");
    private static final Namespace FAILING_NAMESPACE = Namespace.of("failing");

    @Mock
    private AuthHeader authHeader;

    @Mock
    private AtlasRestoreClientBlocking atlasRestoreClient;

    @Mock
    private CassandraRepairHelper cassandraRepairHelper;

    private AtlasRestoreService atlasRestoreService;
    private InMemoryBackupPersister backupPersister;

    @Before
    public void setup() {
        backupPersister = new InMemoryBackupPersister();
        atlasRestoreService =
                new AtlasRestoreService(authHeader, atlasRestoreClient, backupPersister, cassandraRepairHelper);

        storeCompletedBackup(WITH_BACKUP);
        storeCompletedBackup(FAILING_NAMESPACE);
    }

    private void storeCompletedBackup(Namespace namespace) {
        CompletedBackup completedBackup = CompletedBackup.builder()
                .namespace(namespace)
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

    @Test
    public void completeRestoreDoesNotRunNamespacesWithoutCompletedBackup() {
        Set<Namespace> namespaces = atlasRestoreService.completeRestore(ImmutableSet.of(NO_BACKUP));

        assertThat(namespaces).isEmpty();
        verifyNoInteractions(atlasRestoreClient);
    }

    @Test
    public void completeRestoreReturnsSuccessfulNamespaces() {
        Set<Namespace> namespaces = ImmutableSet.of(WITH_BACKUP, FAILING_NAMESPACE);
        Set<CompletedBackup> completedBackups = namespaces.stream()
                .map(backupPersister::getCompletedBackup)
                .flatMap(Optional::stream)
                .collect(Collectors.toSet());

        CompleteRestoreRequest request = CompleteRestoreRequest.of(completedBackups);
        when(atlasRestoreClient.completeRestore(authHeader, request))
                .thenReturn(CompleteRestoreResponse.of(ImmutableSet.of(WITH_BACKUP)));

        Set<Namespace> successfulNamespaces = atlasRestoreService.completeRestore(namespaces);
        assertThat(successfulNamespaces).containsExactly(WITH_BACKUP);
        verify(atlasRestoreClient).completeRestore(authHeader, request);
    }
}
