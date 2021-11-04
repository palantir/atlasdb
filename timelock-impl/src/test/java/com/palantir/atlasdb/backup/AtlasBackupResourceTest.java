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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.api.CompleteBackupRequest;
import com.palantir.atlasdb.timelock.api.CompleteBackupResponse;
import com.palantir.atlasdb.timelock.api.CompletedBackup;
import com.palantir.atlasdb.timelock.api.InProgressBackupToken;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.PrepareBackupRequest;
import com.palantir.atlasdb.timelock.api.PrepareBackupResponse;
import com.palantir.atlasdb.util.TimelockTestUtils;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.tokens.auth.AuthHeader;
import java.net.URL;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;

// TODO(gs): tests with multiple namespaces, including ones where some succeed and some fail
public class AtlasBackupResourceTest {
    private static final int REMOTE_PORT = 4321;
    private static final URL LOCAL = TimelockTestUtils.url("https://localhost:1234");
    private static final URL REMOTE = TimelockTestUtils.url("https://localhost:" + REMOTE_PORT);
    private static final RedirectRetryTargeter TARGETER =
            RedirectRetryTargeter.create(LOCAL, ImmutableList.of(LOCAL, REMOTE));

    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("header");
    private static final Namespace NAMESPACE = Namespace.of("test");
    private static final PrepareBackupRequest PREPARE_BACKUP_REQUEST = PrepareBackupRequest.of(Set.of(NAMESPACE));
    private static final long IMMUTABLE_TIMESTAMP = 1L;
    private static final long BACKUP_START_TIMESTAMP = 2L;
    private static final CompleteBackupResponse EMPTY_COMPLETE_BACKUP_RESPONSE = CompleteBackupResponse.of(Set.of());
    private static final PrepareBackupResponse EMPTY_PREPARE_BACKUP_RESPONSE = PrepareBackupResponse.of(Set.of());

    private final AsyncTimelockService mockTimelock = mock(AsyncTimelockService.class);

    private final AtlasBackupResource atlasBackupService = new AtlasBackupResource(TARGETER, _unused -> mockTimelock);

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
    public void prepareBackupUnsuccessfulWhenLockImmutableTimestampFails() {
        when(mockTimelock.lockImmutableTimestamp(any())).thenThrow(new RuntimeException("agony"));

        assertThat(AtlasFutures.getUnchecked(atlasBackupService.prepareBackup(AUTH_HEADER, PREPARE_BACKUP_REQUEST)))
                .isEqualTo(EMPTY_PREPARE_BACKUP_RESPONSE);
    }

    @Test
    public void completeBackupContainsNamespaceWhenLockIsHeld() {
        InProgressBackupToken backupToken = getValidBackupToken();

        when(mockTimelock.getFreshTimestamp()).thenReturn(3L);
        CompletedBackup expected = CompletedBackup.builder()
                .namespace(backupToken.getNamespace())
                .backupStartTimestamp(backupToken.getBackupStartTimestamp())
                .backupEndTimestamp(3L)
                .build();

        assertThat(AtlasFutures.getUnchecked(
                        atlasBackupService.completeBackup(AUTH_HEADER, completeBackupRequest(backupToken))))
                .isEqualTo(completeBackupResponseWith(expected));
    }

    @Test
    public void completeBackupDoesNotContainNamespaceWhenLockIsLost() {
        InProgressBackupToken backupToken = getInvalidBackupToken();

        assertThat(AtlasFutures.getUnchecked(
                        atlasBackupService.completeBackup(AUTH_HEADER, completeBackupRequest(backupToken))))
                .isEqualTo(EMPTY_COMPLETE_BACKUP_RESPONSE);
    }

    private InProgressBackupToken getValidBackupToken() {
        LockToken lockToken = lockToken();
        InProgressBackupToken backupToken = inProgressBackupToken(lockToken);

        Set<LockToken> singleLockToken = Set.of(lockToken);
        when(mockTimelock.unlock(singleLockToken)).thenReturn(Futures.immediateFuture(singleLockToken));

        return backupToken;
    }

    private InProgressBackupToken getInvalidBackupToken() {
        LockToken lockToken = lockToken();
        InProgressBackupToken backupToken = inProgressBackupToken(lockToken);

        when(mockTimelock.unlock(Set.of(lockToken))).thenReturn(Futures.immediateFuture(Set.of()));

        return backupToken;
    }

    private static PrepareBackupResponse prepareBackupResponseWith(InProgressBackupToken expected) {
        return PrepareBackupResponse.of(Set.of(expected));
    }

    private static CompleteBackupRequest completeBackupRequest(InProgressBackupToken backupToken) {
        return CompleteBackupRequest.of(Set.of(backupToken));
    }

    private static CompleteBackupResponse completeBackupResponseWith(CompletedBackup expected) {
        return CompleteBackupResponse.of(Set.of(expected));
    }

    private static InProgressBackupToken inProgressBackupToken(LockToken lockToken) {
        return InProgressBackupToken.builder()
                .namespace(NAMESPACE)
                .lockToken(lockToken)
                .immutableTimestamp(IMMUTABLE_TIMESTAMP)
                .backupStartTimestamp(BACKUP_START_TIMESTAMP)
                .build();
    }

    private static LockToken lockToken() {
        UUID requestId = UUID.randomUUID();
        return LockToken.of(requestId);
    }
}
