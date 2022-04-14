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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.backup.api.AtlasRestoreClient;
import com.palantir.atlasdb.backup.api.AtlasService;
import com.palantir.atlasdb.backup.api.CompleteRestoreRequest;
import com.palantir.atlasdb.backup.api.CompleteRestoreResponse;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.backup.api.RestoredService;
import com.palantir.atlasdb.backup.api.ServiceId;
import com.palantir.atlasdb.cassandra.backup.CassandraRepairHelper;
import com.palantir.atlasdb.cassandra.backup.RangesForRepair;
import com.palantir.atlasdb.timelock.api.DisableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.DisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.SuccessfulDisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulDisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.management.TimeLockManagementService;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.tokens.auth.AuthHeader;
import java.util.Optional;
import java.util.Set;
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
    private static final Namespace WITH_BACKUP_NS = Namespace.of("with-backup");
    private static final AtlasService WITH_BACKUP = AtlasService.of(ServiceId.of("a"), WITH_BACKUP_NS);
    private static final Namespace NO_BACKUP_NS = Namespace.of("no-backup");
    private static final AtlasService NO_BACKUP = AtlasService.of(ServiceId.of("b"), NO_BACKUP_NS);
    private static final AtlasService FAILING_NAMESPACE = AtlasService.of(ServiceId.of("c"), Namespace.of("failing"));
    private static final long BACKUP_START_TIMESTAMP = 2L;
    private static final String BACKUP_ID = "backup-19890526215242";

    @Mock
    private AuthHeader authHeader;

    @Mock
    private AtlasRestoreClient atlasRestoreClient;

    @Mock
    private TimeLockManagementService timeLockManagementService;

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

    private void storeCompletedBackup(AtlasService atlasService) {
        CompletedBackup completedBackup = CompletedBackup.builder()
                .atlasService(atlasService)
                .immutableTimestamp(1L)
                .backupStartTimestamp(BACKUP_START_TIMESTAMP)
                .backupEndTimestamp(3L)
                .build();
        backupPersister.storeCompletedBackup(completedBackup);
    }

    @Test
    public void cannotRestoreTwiceToSameAtlasService() {
        AtlasService severeData = AtlasService.of(ServiceId.of("a"), Namespace.of("severe-data"));
        storeCompletedBackup(severeData);

        AtlasService corruption = AtlasService.of(ServiceId.of("b"), Namespace.of("corruption"));
        RestoreRequest firstRequest = RestoreRequest.builder()
                .oldAtlasService(WITH_BACKUP)
                .newAtlasService(corruption)
                .build();
        RestoreRequest secondRequest = RestoreRequest.builder()
                .oldAtlasService(severeData)
                .newAtlasService(corruption)
                .build();
        Set<RestoreRequest> requests = ImmutableSet.of(firstRequest, secondRequest);

        assertThatThrownBy(() -> atlasRestoreService.prepareRestore(requests, BACKUP_ID))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("Restore cannot safely proceed.");
    }

    @Test
    public void restoresToNewAtlasServiceCorrectly() {
        RestoreRequest restoreRequest = RestoreRequest.builder()
                .oldAtlasService(WITH_BACKUP)
                .newAtlasService(NO_BACKUP)
                .build();

        // prepare
        DisableNamespacesResponse successfulDisable =
                DisableNamespacesResponse.successful(SuccessfulDisableNamespacesResponse.of(BACKUP_ID));
        DisableNamespacesRequest request = DisableNamespacesRequest.of(ImmutableSet.of(NO_BACKUP_NS), BACKUP_ID);
        when(timeLockManagementService.disableTimelock(authHeader, request)).thenReturn(successfulDisable);

        Set<AtlasService> disabledAtlasServices =
                atlasRestoreService.prepareRestore(ImmutableSet.of(restoreRequest), BACKUP_ID);
        assertThat(disabledAtlasServices).containsExactly(NO_BACKUP);

        // repair
        BiConsumer<String, RangesForRepair> doNothingConsumer = (_unused1, _unused2) -> {};

        Set<AtlasService> repairedAtlasServices =
                atlasRestoreService.repairInternalTables(ImmutableSet.of(restoreRequest), doNothingConsumer);
        assertThat(repairedAtlasServices).containsExactly(NO_BACKUP);

        verify(cassandraRepairHelper).repairInternalTables(NO_BACKUP, doNothingConsumer);
        verify(cassandraRepairHelper).repairTransactionsTables(eq(NO_BACKUP), anyList(), eq(doNothingConsumer));
        verify(cassandraRepairHelper).cleanTransactionsTables(eq(NO_BACKUP), eq(BACKUP_START_TIMESTAMP), anyList());
        verifyNoMoreInteractions(cassandraRepairHelper);

        // complete
        CompletedBackup completedBackup =
                backupPersister.getCompletedBackup(WITH_BACKUP).orElseThrow();
        CompleteRestoreRequest completeRestoreRequest =
                CompleteRestoreRequest.of(ImmutableSet.of(RestoredService.of(NO_BACKUP, completedBackup)));
        when(atlasRestoreClient.completeRestore(authHeader, completeRestoreRequest))
                .thenReturn(CompleteRestoreResponse.of(ImmutableSet.of(NO_BACKUP)));
        ReenableNamespacesRequest reenableRequest =
                ReenableNamespacesRequest.of(ImmutableSet.of(NO_BACKUP_NS), BACKUP_ID);

        Set<AtlasService> completedAtlasServices =
                atlasRestoreService.completeRestore(ImmutableSet.of(restoreRequest), BACKUP_ID);
        assertThat(completedAtlasServices).containsExactly(NO_BACKUP);
        verify(atlasRestoreClient).completeRestore(authHeader, completeRestoreRequest);
        verify(timeLockManagementService).reenableTimelock(authHeader, reenableRequest);
    }

    @Test
    public void prepareReturnsOnlyCompletedBackups() {
        DisableNamespacesResponse successfulDisable =
                DisableNamespacesResponse.successful(SuccessfulDisableNamespacesResponse.of(BACKUP_ID));
        DisableNamespacesRequest request = DisableNamespacesRequest.of(ImmutableSet.of(WITH_BACKUP_NS), BACKUP_ID);
        when(timeLockManagementService.disableTimelock(authHeader, request)).thenReturn(successfulDisable);

        Set<AtlasService> disabledAtlasServices = atlasRestoreService.prepareRestore(
                ImmutableSet.of(restoreRequest(WITH_BACKUP), restoreRequest(NO_BACKUP)), BACKUP_ID);
        assertThat(disabledAtlasServices).containsExactly(WITH_BACKUP);
    }

    @Test
    public void prepareBackupFailsIfDisableFails() {
        DisableNamespacesResponse failedDisable = DisableNamespacesResponse.unsuccessful(
                UnsuccessfulDisableNamespacesResponse.of(ImmutableSet.of(WITH_BACKUP_NS), ImmutableSet.of()));
        DisableNamespacesRequest request = DisableNamespacesRequest.of(ImmutableSet.of(WITH_BACKUP_NS), BACKUP_ID);
        when(timeLockManagementService.disableTimelock(authHeader, request)).thenReturn(failedDisable);

        Set<AtlasService> disabledAtlasServices = atlasRestoreService.prepareRestore(
                ImmutableSet.of(restoreRequest(WITH_BACKUP), restoreRequest(NO_BACKUP)), BACKUP_ID);
        assertThat(disabledAtlasServices).isEmpty();
    }

    @Test
    public void repairsOnlyWhenBackupPresentAndDisableSuccessful() {
        BiConsumer<String, RangesForRepair> doNothingConsumer = (_unused1, _unused2) -> {};

        Set<AtlasService> repairedAtlasServices = atlasRestoreService.repairInternalTables(
                ImmutableSet.of(restoreRequest(WITH_BACKUP), restoreRequest(NO_BACKUP)), doNothingConsumer);
        assertThat(repairedAtlasServices).containsExactly(WITH_BACKUP);

        verify(cassandraRepairHelper).repairInternalTables(WITH_BACKUP, doNothingConsumer);
        verify(cassandraRepairHelper).repairTransactionsTables(eq(WITH_BACKUP), anyList(), eq(doNothingConsumer));
        verify(cassandraRepairHelper).cleanTransactionsTables(eq(WITH_BACKUP), eq(BACKUP_START_TIMESTAMP), anyList());
        verifyNoMoreInteractions(cassandraRepairHelper);
    }

    @Test
    public void completesRestoreAfterFastForwardingTimestamp() {
        Set<AtlasService> atlasServices = ImmutableSet.of(WITH_BACKUP);
        Set<RestoredService> restoredServices = ImmutableSet.of(RestoredService.of(
                WITH_BACKUP, backupPersister.getCompletedBackup(WITH_BACKUP).orElseThrow()));

        CompleteRestoreRequest completeRequest = CompleteRestoreRequest.of(restoredServices);
        when(atlasRestoreClient.completeRestore(authHeader, completeRequest))
                .thenReturn(CompleteRestoreResponse.of(ImmutableSet.of(WITH_BACKUP)));

        ReenableNamespacesRequest reenableRequest =
                ReenableNamespacesRequest.of(ImmutableSet.of(WITH_BACKUP_NS), BACKUP_ID);

        Set<AtlasService> successfulAtlasServices =
                atlasRestoreService.completeRestore(ImmutableSet.of(restoreRequest(WITH_BACKUP)), BACKUP_ID);
        assertThat(successfulAtlasServices).containsExactly(WITH_BACKUP);

        InOrder inOrder = Mockito.inOrder(atlasRestoreClient, timeLockManagementService);
        inOrder.verify(atlasRestoreClient).completeRestore(authHeader, completeRequest);
        inOrder.verify(timeLockManagementService).reenableTimelock(authHeader, reenableRequest);
    }

    @Test
    public void completeRestoreDoesNotRunNamespacesWithoutCompletedBackup() {
        Set<AtlasService> atlasServices =
                atlasRestoreService.completeRestore(ImmutableSet.of(restoreRequest(NO_BACKUP)), BACKUP_ID);

        assertThat(atlasServices).isEmpty();
        verifyNoInteractions(atlasRestoreClient);
        verifyNoInteractions(timeLockManagementService);
    }

    @Test
    public void completeRestoreReturnsSuccessfulNamespaces() {
        Set<AtlasService> atlasServices = ImmutableSet.of(WITH_BACKUP, FAILING_NAMESPACE);
        Set<RestoredService> restoredServices = atlasServices.stream()
                .map(service -> backupPersister.getCompletedBackup(service))
                .flatMap(Optional::stream)
                .map(completedBackup -> RestoredService.of(completedBackup.getAtlasService(), completedBackup))
                .collect(Collectors.toSet());

        CompleteRestoreRequest request = CompleteRestoreRequest.of(restoredServices);
        when(atlasRestoreClient.completeRestore(authHeader, request))
                .thenReturn(CompleteRestoreResponse.of(ImmutableSet.of(WITH_BACKUP)));

        ReenableNamespacesRequest reenableRequest =
                ReenableNamespacesRequest.of(ImmutableSet.of(WITH_BACKUP_NS), BACKUP_ID);

        Set<RestoreRequest> requests =
                atlasServices.stream().map(this::restoreRequest).collect(Collectors.toSet());
        Set<AtlasService> successfulAtlasServices = atlasRestoreService.completeRestore(requests, BACKUP_ID);
        assertThat(successfulAtlasServices).containsExactly(WITH_BACKUP);
        verify(atlasRestoreClient).completeRestore(authHeader, request);
        verify(timeLockManagementService).reenableTimelock(authHeader, reenableRequest);
    }

    private RestoreRequest restoreRequest(AtlasService atlasService) {
        return RestoreRequest.builder()
                .oldAtlasService(atlasService)
                .newAtlasService(atlasService)
                .build();
    }
}
