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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.backup.api.BackupId;
import com.palantir.atlasdb.backup.api.CompleteBackupRequest;
import com.palantir.atlasdb.backup.api.CompleteBackupResponse;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.backup.api.InProgressBackupToken;
import com.palantir.atlasdb.backup.api.PrepareBackupRequest;
import com.palantir.atlasdb.backup.api.PrepareBackupResponse;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.util.TimelockTestUtils;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.tokens.auth.AuthHeader;
import java.net.URL;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;

public class AtlasBackupResourceTest {
    private static final int REMOTE_PORT = 4321;
    private static final URL LOCAL = TimelockTestUtils.url("https://localhost:1234");
    private static final URL REMOTE = TimelockTestUtils.url("https://localhost:" + REMOTE_PORT);
    private static final RedirectRetryTargeter TARGETER = RedirectRetryTargeter.create(LOCAL, List.of(LOCAL, REMOTE));

    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("header");
    private static final BackupId BACKUP_ID = BackupId.of("backup");
    private static final Namespace NAMESPACE = Namespace.of("test");
    private static final Namespace OTHER_NAMESPACE = Namespace.of("other");
    private static final PrepareBackupRequest PREPARE_BACKUP_REQUEST =
            PrepareBackupRequest.of(BACKUP_ID, ImmutableSet.of(NAMESPACE));
    private static final long IMMUTABLE_TIMESTAMP = 1L;
    private static final long BACKUP_START_TIMESTAMP = 2L;
    private static final CompleteBackupResponse EMPTY_COMPLETE_BACKUP_RESPONSE =
            CompleteBackupResponse.of(ImmutableSet.of());

    private final AsyncTimelockService mockTimelock = mock(AsyncTimelockService.class);
    private final AsyncTimelockService otherTimelock = mock(AsyncTimelockService.class);

    private final AtlasBackupResource atlasBackupService =
            new AtlasBackupResource(TARGETER, str -> str.equals("test") ? mockTimelock : otherTimelock);

    @Test
    public void preparesBackupSuccessfully() {
        LockToken lockToken = lockToken();
        when(mockTimelock.lockImmutableTimestamp(any()))
                .thenReturn(LockImmutableTimestampResponse.of(IMMUTABLE_TIMESTAMP, lockToken));
        when(mockTimelock.getFreshTimestamp()).thenReturn(BACKUP_START_TIMESTAMP);

        InProgressBackupToken expectedBackupToken = inProgressBackupToken(lockToken);

        assertThat(AtlasFutures.getUnchecked(atlasBackupService.prepareBackup(AUTH_HEADER, PREPARE_BACKUP_REQUEST)))
                .isEqualTo(prepareBackupResponseWith(expectedBackupToken));
    }

    @Test
    public void completeBackupContainsNamespaceWhenLockIsHeld() {
        when(mockTimelock.getFreshTimestamp()).thenReturn(3L);

        InProgressBackupToken backupToken = validBackupToken();
        CompletedBackup expected = completedBackup(backupToken);

        assertThat(AtlasFutures.getUnchecked(
                        atlasBackupService.completeBackup(AUTH_HEADER, completeBackupRequest(backupToken))))
                .isEqualTo(completeBackupResponseWith(expected));
    }

    @Test
    public void completeBackupDoesNotContainNamespaceWhenLockIsLost() {
        InProgressBackupToken backupToken = invalidBackupToken();

        assertThat(AtlasFutures.getUnchecked(
                        atlasBackupService.completeBackup(AUTH_HEADER, completeBackupRequest(backupToken))))
                .isEqualTo(EMPTY_COMPLETE_BACKUP_RESPONSE);
    }

    @Test
    public void completeBackupFiltersOutUnsuccessfulNamespaces() {
        when(mockTimelock.getFreshTimestamp()).thenReturn(3L);

        InProgressBackupToken validToken = validBackupToken();
        InProgressBackupToken invalidToken = invalidBackupToken(OTHER_NAMESPACE, otherTimelock);
        CompletedBackup expected = completedBackup(validToken);

        assertThat(AtlasFutures.getUnchecked(atlasBackupService.completeBackup(
                        AUTH_HEADER, completeBackupRequest(validToken, invalidToken))))
                .isEqualTo(completeBackupResponseWith(expected));
    }

    private InProgressBackupToken validBackupToken() {
        LockToken lockToken = lockToken();
        InProgressBackupToken backupToken = inProgressBackupToken(lockToken);

        Set<LockToken> singleLockToken = ImmutableSet.of(lockToken);
        when(mockTimelock.unlock(singleLockToken)).thenReturn(Futures.immediateFuture(singleLockToken));

        return backupToken;
    }

    private InProgressBackupToken invalidBackupToken() {
        return invalidBackupToken(NAMESPACE, mockTimelock);
    }

    private static InProgressBackupToken invalidBackupToken(Namespace namespace, AsyncTimelockService timelock) {
        LockToken lockToken = lockToken();
        InProgressBackupToken backupToken = inProgressBackupToken(namespace, lockToken);

        when(timelock.unlock(ImmutableSet.of(lockToken))).thenReturn(Futures.immediateFuture(ImmutableSet.of()));

        return backupToken;
    }

    private static PrepareBackupResponse prepareBackupResponseWith(InProgressBackupToken expected) {
        return PrepareBackupResponse.of(ImmutableSet.of(expected));
    }

    private static CompleteBackupRequest completeBackupRequest(InProgressBackupToken... backupTokens) {
        return CompleteBackupRequest.of(ImmutableSet.copyOf(backupTokens));
    }

    private static CompleteBackupResponse completeBackupResponseWith(CompletedBackup expected) {
        return CompleteBackupResponse.of(ImmutableSet.of(expected));
    }

    private static InProgressBackupToken inProgressBackupToken(LockToken lockToken) {
        return inProgressBackupToken(NAMESPACE, lockToken);
    }

    private static InProgressBackupToken inProgressBackupToken(Namespace namespace, LockToken lockToken) {
        return InProgressBackupToken.builder()
                .backupId(BACKUP_ID)
                .namespace(namespace)
                .lockToken(lockToken)
                .immutableTimestamp(IMMUTABLE_TIMESTAMP)
                .backupStartTimestamp(BACKUP_START_TIMESTAMP)
                .build();
    }

    private static LockToken lockToken() {
        UUID requestId = UUID.randomUUID();
        return LockToken.of(requestId);
    }

    private static CompletedBackup completedBackup(InProgressBackupToken backupToken) {
        return CompletedBackup.builder()
                .backupId(backupToken.getBackupId())
                .namespace(backupToken.getNamespace())
                .backupStartTimestamp(backupToken.getBackupStartTimestamp())
                .backupEndTimestamp(3L)
                .build();
    }
}
