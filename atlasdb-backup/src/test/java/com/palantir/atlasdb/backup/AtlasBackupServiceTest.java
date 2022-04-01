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
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.backup.api.AtlasBackupClient;
import com.palantir.atlasdb.backup.api.CompleteBackupRequest;
import com.palantir.atlasdb.backup.api.CompleteBackupResponse;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.backup.api.InProgressBackupToken;
import com.palantir.atlasdb.backup.api.PrepareBackupRequest;
import com.palantir.atlasdb.backup.api.PrepareBackupResponse;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.lock.client.LockRefresher;
import com.palantir.lock.v2.LockToken;
import com.palantir.tokens.auth.AuthHeader;
import java.util.Set;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AtlasBackupServiceTest {
    private static final Namespace NAMESPACE = Namespace.of("foo");
    private static final Namespace OTHER_NAMESPACE = Namespace.of("other");
    private static final InProgressBackupToken IN_PROGRESS = inProgressBackupToken(NAMESPACE);

    @Mock
    private AuthHeader authHeader;

    @Mock
    private AtlasBackupClient atlasBackupClient;

    @Mock
    private CoordinationServiceRecorder coordinationServiceRecorder;

    @Mock
    private LockRefresher<InProgressBackupToken> lockRefresher;

    private AtlasBackupService atlasBackupService;
    private BackupPersister backupPersister;

    @Before
    public void setup() {
        backupPersister = new InMemoryBackupPersister();
        atlasBackupService = new AtlasBackupService(
                authHeader, atlasBackupClient, coordinationServiceRecorder, backupPersister, lockRefresher);
    }

    @Test
    public void prepareBackupReturnsSuccessfulNamespaces() {
        when(atlasBackupClient.prepareBackup(
                        authHeader, PrepareBackupRequest.of(ImmutableSet.of(NAMESPACE, OTHER_NAMESPACE))))
                .thenReturn(PrepareBackupResponse.of(ImmutableSet.of(IN_PROGRESS)));

        assertThat(atlasBackupService.prepareBackup(ImmutableSet.of(NAMESPACE, OTHER_NAMESPACE)))
                .containsExactly(NAMESPACE);
    }

    @Test
    public void prepareBackupRegistersLockForRefresh() {
        Set<InProgressBackupToken> tokens = ImmutableSet.of(IN_PROGRESS);
        when(atlasBackupClient.prepareBackup(
                        authHeader, PrepareBackupRequest.of(ImmutableSet.of(NAMESPACE, OTHER_NAMESPACE))))
                .thenReturn(PrepareBackupResponse.of(tokens));

        atlasBackupService.prepareBackup(ImmutableSet.of(NAMESPACE, OTHER_NAMESPACE));
        verify(lockRefresher).registerLocks(tokens);
    }

    @Test
    public void completeBackupDoesNotRunUnpreparedNamespaces() {
        when(atlasBackupClient.completeBackup(authHeader, CompleteBackupRequest.of(ImmutableSet.of())))
                .thenReturn(CompleteBackupResponse.of(ImmutableSet.of()));

        assertThat(atlasBackupService.completeBackup(ImmutableSet.of(OTHER_NAMESPACE)))
                .isEmpty();
    }

    @Test
    public void completeBackupReturnsSuccessfulNamespaces() {
        InProgressBackupToken otherInProgress = inProgressBackupToken(OTHER_NAMESPACE);
        Set<Namespace> namespaces = ImmutableSet.of(NAMESPACE, OTHER_NAMESPACE);

        when(atlasBackupClient.prepareBackup(authHeader, PrepareBackupRequest.of(namespaces)))
                .thenReturn(PrepareBackupResponse.of(ImmutableSet.of(IN_PROGRESS, otherInProgress)));

        when(atlasBackupClient.completeBackup(
                        authHeader, CompleteBackupRequest.of(ImmutableSet.of(IN_PROGRESS, otherInProgress))))
                .thenReturn(CompleteBackupResponse.of(ImmutableSet.of(completedBackup())));

        atlasBackupService.prepareBackup(namespaces);

        assertThat(atlasBackupService.completeBackup(namespaces)).containsExactly(NAMESPACE);
    }

    @Test
    public void completeBackupUnregistersLocks() {
        Set<Namespace> oneNamespace = ImmutableSet.of(NAMESPACE);
        Set<InProgressBackupToken> tokens = ImmutableSet.of(IN_PROGRESS);
        when(atlasBackupClient.prepareBackup(authHeader, PrepareBackupRequest.of(oneNamespace)))
                .thenReturn(PrepareBackupResponse.of(tokens));

        CompletedBackup completedBackup = completedBackup();
        when(atlasBackupClient.completeBackup(authHeader, CompleteBackupRequest.of(tokens)))
                .thenReturn(CompleteBackupResponse.of(ImmutableSet.of(completedBackup)));

        atlasBackupService.prepareBackup(oneNamespace);
        atlasBackupService.completeBackup(oneNamespace);

        verify(lockRefresher).unregisterLocks(tokens);
    }

    @Test
    public void completeBackupStoresBackupInfoAndMetadata() {
        ImmutableSet<Namespace> oneNamespace = ImmutableSet.of(NAMESPACE);
        when(atlasBackupClient.prepareBackup(authHeader, PrepareBackupRequest.of(oneNamespace)))
                .thenReturn(PrepareBackupResponse.of(ImmutableSet.of(IN_PROGRESS)));

        CompletedBackup completedBackup = completedBackup();
        when(atlasBackupClient.completeBackup(authHeader, CompleteBackupRequest.of(ImmutableSet.of(IN_PROGRESS))))
                .thenReturn(CompleteBackupResponse.of(ImmutableSet.of(completedBackup)));

        atlasBackupService.prepareBackup(oneNamespace);
        atlasBackupService.completeBackup(oneNamespace);

        verify(coordinationServiceRecorder).storeFastForwardState(completedBackup);
        assertThat(backupPersister.getCompletedBackup(NAMESPACE)).contains(completedBackup);
    }

    private static CompletedBackup completedBackup() {
        return CompletedBackup.builder()
                .namespace(NAMESPACE)
                .immutableTimestamp(1L)
                .backupStartTimestamp(2L)
                .backupEndTimestamp(3L)
                .build();
    }

    private static InProgressBackupToken inProgressBackupToken(Namespace namespace) {
        return InProgressBackupToken.builder()
                .namespace(namespace)
                .immutableTimestamp(1L)
                .backupStartTimestamp(2L)
                .lockToken(LockToken.of(UUID.randomUUID()))
                .build();
    }
}
