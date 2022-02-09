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
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.backup.api.AtlasRestoreClientBlocking;
import com.palantir.atlasdb.backup.api.CompleteRestoreRequest;
import com.palantir.atlasdb.backup.api.CompleteRestoreResponse;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.cassandra.backup.CassandraRepairHelper;
import com.palantir.atlasdb.cassandra.backup.RangesForRepair;
import com.palantir.atlasdb.timelock.api.DisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.SuccessfulDisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.SuccessfulReenableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulDisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.management.TimeLockManagementServiceBlocking;
import com.palantir.tokens.auth.AuthHeader;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AtlasRestoreServiceTest {
    private static final Namespace WITH_BACKUP = Namespace.of("with-backup");
    private static final Namespace NO_BACKUP = Namespace.of("no-backup");
    private static final Namespace FAILING_NAMESPACE = Namespace.of("failing");
    private static final long BACKUP_START_TIMESTAMP = 2L;
    private static final UUID LOCK_ID = new UUID(12, 9);

    @Mock
    private AuthHeader authHeader;

    @Mock
    private AtlasRestoreClientBlocking atlasRestoreClient;

    @Mock
    private TimeLockManagementServiceBlocking timeLockManagementService;

    @Mock
    private CassandraRepairHelper cassandraRepairHelper;

    private AtlasRestoreService atlasRestoreService;
    private InMemoryBackupPersister backupPersister;

    @Before
    public void setup() {
        backupPersister = new InMemoryBackupPersister();
        atlasRestoreService = new AtlasRestoreService(
                authHeader, atlasRestoreClient, timeLockManagementService, backupPersister, cassandraRepairHelper);

        storeCompletedBackup(WITH_BACKUP);
        storeCompletedBackup(FAILING_NAMESPACE);
    }

    private void storeCompletedBackup(Namespace namespace) {
        CompletedBackup completedBackup = CompletedBackup.builder()
                .namespace(namespace)
                .immutableTimestamp(1L)
                .backupStartTimestamp(BACKUP_START_TIMESTAMP)
                .backupEndTimestamp(3L)
                .build();
        backupPersister.storeCompletedBackup(completedBackup);
    }

    @Test
    public void prepareRestoreReturnsOnlyCompletedBackups() {
        DisableNamespacesResponse successfulDisable =
                DisableNamespacesResponse.successful(SuccessfulDisableNamespacesResponse.of(LOCK_ID));
        when(timeLockManagementService.disableTimelock(authHeader, ImmutableSet.of(WITH_BACKUP)))
                .thenReturn(successfulDisable);

        DisableNamespacesResponse actualDisable =
                atlasRestoreService.prepareRestore(ImmutableSet.of(WITH_BACKUP, NO_BACKUP));
        assertThat(actualDisable).isEqualTo(successfulDisable);
    }

    @Test
    public void prepareRestorePersistsLockId() {
        Namespace otherSuccessfulNamespace = Namespace.valueOf("success2");
        storeCompletedBackup(otherSuccessfulNamespace);
        DisableNamespacesResponse successfulDisable =
                DisableNamespacesResponse.successful(SuccessfulDisableNamespacesResponse.of(LOCK_ID));
        when(timeLockManagementService.disableTimelock(
                        authHeader, ImmutableSet.of(WITH_BACKUP, otherSuccessfulNamespace)))
                .thenReturn(successfulDisable);

        DisableNamespacesResponse actualDisable =
                atlasRestoreService.prepareRestore(ImmutableSet.of(WITH_BACKUP, otherSuccessfulNamespace));

        assertThat(actualDisable).isEqualTo(successfulDisable);
        assertThat(backupPersister.getRestoreLockId(WITH_BACKUP)).contains(LOCK_ID);
        assertThat(backupPersister.getRestoreLockId(otherSuccessfulNamespace)).contains(LOCK_ID);
    }

    @Test
    public void prepareRestoreFailsIfDisableFails() {
        DisableNamespacesResponse failedDisable = DisableNamespacesResponse.unsuccessful(
                UnsuccessfulDisableNamespacesResponse.of(ImmutableSet.of(WITH_BACKUP), ImmutableSet.of()));
        when(timeLockManagementService.disableTimelock(authHeader, ImmutableSet.of(WITH_BACKUP)))
                .thenReturn(failedDisable);

        DisableNamespacesResponse actualDisable =
                atlasRestoreService.prepareRestore(ImmutableSet.of(WITH_BACKUP, NO_BACKUP));
        assertThat(actualDisable).isEqualTo(failedDisable);
    }

    @Test
    public void repairsOnlyWhenBackupPresentAndDisableSuccessful() {
        BiConsumer<String, RangesForRepair> doNothingConsumer = (_unused1, _unused2) -> {};

        Set<Namespace> repairedNamespaces =
                atlasRestoreService.repairInternalTables(ImmutableSet.of(WITH_BACKUP, NO_BACKUP), doNothingConsumer);
        assertThat(repairedNamespaces).containsExactly(WITH_BACKUP);

        verify(cassandraRepairHelper).repairInternalTables(WITH_BACKUP, doNothingConsumer);
        verify(cassandraRepairHelper).repairTransactionsTables(eq(WITH_BACKUP), anyList(), eq(doNothingConsumer));
        verify(cassandraRepairHelper).cleanTransactionsTables(eq(WITH_BACKUP), eq(BACKUP_START_TIMESTAMP), anyList());
        verifyNoMoreInteractions(cassandraRepairHelper);
    }

    @Test
    public void completesRestoreAfterFastForwardingTimestamp() {
        Set<Namespace> namespaces = ImmutableSet.of(WITH_BACKUP);
        Set<CompletedBackup> completedBackups = namespaces.stream()
                .map(backupPersister::getCompletedBackup)
                .flatMap(Optional::stream)
                .collect(Collectors.toSet());
        backupPersister.storeRestoreLockId(WITH_BACKUP, LOCK_ID);

        CompleteRestoreRequest completeRequest = CompleteRestoreRequest.of(completedBackups);
        when(atlasRestoreClient.completeRestore(authHeader, completeRequest))
                .thenReturn(CompleteRestoreResponse.of(ImmutableSet.of(WITH_BACKUP)));

        ReenableNamespacesRequest reenableRequest = ReenableNamespacesRequest.of(namespaces, LOCK_ID);
        when(timeLockManagementService.reenableTimelock(authHeader, reenableRequest))
                .thenReturn(ReenableNamespacesResponse.successful(SuccessfulReenableNamespacesResponse.of(true)));

        Set<Namespace> successfulNamespaces = atlasRestoreService.completeRestore(reenableRequest);
        assertThat(successfulNamespaces).containsExactly(WITH_BACKUP);

        InOrder inOrder = Mockito.inOrder(atlasRestoreClient, timeLockManagementService);
        inOrder.verify(atlasRestoreClient).completeRestore(authHeader, completeRequest);
        inOrder.verify(timeLockManagementService).reenableTimelock(authHeader, reenableRequest);
    }

    @Test
    public void completeRestoreDoesNotRunNamespacesWithoutCompletedBackup() {
        ReenableNamespacesRequest reenableRequest = ReenableNamespacesRequest.of(ImmutableSet.of(NO_BACKUP), LOCK_ID);

        Set<Namespace> namespaces = atlasRestoreService.completeRestore(reenableRequest);

        assertThat(namespaces).isEmpty();
        verifyNoInteractions(atlasRestoreClient);
        verifyNoInteractions(timeLockManagementService);
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
        backupPersister.storeRestoreLockId(WITH_BACKUP, LOCK_ID);

        ReenableNamespacesRequest reenableRequest = ReenableNamespacesRequest.of(ImmutableSet.of(WITH_BACKUP), LOCK_ID);
        when(timeLockManagementService.reenableTimelock(authHeader, reenableRequest))
                .thenReturn(ReenableNamespacesResponse.successful(SuccessfulReenableNamespacesResponse.of(true)));

        Set<Namespace> successfulNamespaces =
                atlasRestoreService.completeRestore(ReenableNamespacesRequest.of(namespaces, LOCK_ID));
        assertThat(successfulNamespaces).containsExactly(WITH_BACKUP);
        verify(atlasRestoreClient).completeRestore(authHeader, request);
    }

    @Test
    public void completeRestoreUsesPersistedLockId() {
        Set<Namespace> namespaces = ImmutableSet.of(WITH_BACKUP);
        Set<CompletedBackup> completedBackups = namespaces.stream()
                .map(backupPersister::getCompletedBackup)
                .flatMap(Optional::stream)
                .collect(Collectors.toSet());
        UUID actualLockId = new UUID(23, 223);
        backupPersister.storeRestoreLockId(WITH_BACKUP, actualLockId);

        CompleteRestoreRequest request = CompleteRestoreRequest.of(completedBackups);
        when(atlasRestoreClient.completeRestore(authHeader, request))
                .thenReturn(CompleteRestoreResponse.of(namespaces));
        ReenableNamespacesRequest reenableRequest = ReenableNamespacesRequest.of(namespaces, LOCK_ID);
        ReenableNamespacesRequest expectedRequest = ReenableNamespacesRequest.of(namespaces, actualLockId);
        when(timeLockManagementService.reenableTimelock(authHeader, expectedRequest))
                .thenReturn(ReenableNamespacesResponse.successful(SuccessfulReenableNamespacesResponse.of(true)));

        Set<Namespace> successfulNamespaces = atlasRestoreService.completeRestore(reenableRequest);

        assertThat(successfulNamespaces).containsExactly(WITH_BACKUP);
        verify(timeLockManagementService).reenableTimelock(authHeader, expectedRequest);
    }
}
