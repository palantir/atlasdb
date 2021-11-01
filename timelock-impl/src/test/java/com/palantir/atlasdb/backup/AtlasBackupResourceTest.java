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
import com.palantir.atlasdb.timelock.api.BackupToken;
import com.palantir.atlasdb.timelock.api.CompleteBackupRequest;
import com.palantir.atlasdb.timelock.api.CompleteBackupResponse;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.PrepareBackupRequest;
import com.palantir.atlasdb.timelock.api.PrepareBackupResponse;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.tokens.auth.AuthHeader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Set;
import java.util.UUID;
import org.assertj.core.api.Assertions;
import org.junit.Test;

// TODO(gs): tests with multiple namespaces, including ones where some succeed and some fail
public class AtlasBackupResourceTest {
    private static final int REMOTE_PORT = 4321;
    private static final URL LOCAL = url("https://localhost:1234");
    private static final URL REMOTE = url("https://localhost:" + REMOTE_PORT);
    private static final RedirectRetryTargeter TARGETER =
            RedirectRetryTargeter.create(LOCAL, ImmutableList.of(LOCAL, REMOTE));

    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("header");
    private static final Namespace NAMESPACE = Namespace.of("test");
    private static final long IMMUTABLE_TIMESTAMP = 1L;
    private static final long BACKUP_START_TIMESTAMP = 2L;

    private final AsyncTimelockService mockTimelock = mock(AsyncTimelockService.class);

    private final AtlasBackupResource atlasBackupService = new AtlasBackupResource(TARGETER, _unused -> mockTimelock);

    @Test
    public void preparesBackupSuccessfully() {
        LockToken lockToken = lockToken();
        when(mockTimelock.lockImmutableTimestamp(any()))
                .thenReturn(LockImmutableTimestampResponse.of(IMMUTABLE_TIMESTAMP, lockToken));
        when(mockTimelock.getFreshTimestamp()).thenReturn(BACKUP_START_TIMESTAMP);

        BackupToken expectedBackupToken = backupToken(lockToken);
        PrepareBackupResponse expected = PrepareBackupResponse.of(Set.of(expectedBackupToken));

        PrepareBackupResponse response = AtlasFutures.getUnchecked(
                atlasBackupService.prepareBackup(AUTH_HEADER, PrepareBackupRequest.of(Set.of(NAMESPACE))));

        assertThat(response).isEqualTo(expected);
    }

    @Test
    public void prepareBackupUnsuccessfulWhenLockImmutableTimestampFails() {
        when(mockTimelock.lockImmutableTimestamp(any())).thenThrow(new RuntimeException("agony"));

        PrepareBackupResponse response = AtlasFutures.getUnchecked(
                atlasBackupService.prepareBackup(AUTH_HEADER, PrepareBackupRequest.of(Set.of(NAMESPACE))));
        PrepareBackupResponse expected = PrepareBackupResponse.of(Set.of());
        assertThat(response).isEqualTo(expected);
    }

    @Test
    public void completeBackupReturnsTrueWhenLockIsHeld() {
        BackupToken backupToken = getValidBackupToken();

        when(mockTimelock.getFreshTimestamp()).thenReturn(3L);
        BackupToken expected =
                BackupToken.builder().from(backupToken).backupEndTimestamp(3L).build();

        Assertions.assertThat(AtlasFutures.getUnchecked(
                        atlasBackupService.completeBackup(AUTH_HEADER, CompleteBackupRequest.of(Set.of(backupToken)))))
                .isEqualTo(CompleteBackupResponse.of(Set.of(expected)));
    }

    @Test
    public void completeBackupReturnsFalseWhenLockIsLost() {
        BackupToken backupToken = getInvalidBackupToken();

        Assertions.assertThat(AtlasFutures.getUnchecked(
                        atlasBackupService.completeBackup(AUTH_HEADER, CompleteBackupRequest.of(Set.of(backupToken)))))
                .isEqualTo(CompleteBackupResponse.of(Set.of()));
    }

    private BackupToken getValidBackupToken() {
        LockToken lockToken = lockToken();
        BackupToken backupToken = backupToken(lockToken);

        Set<LockToken> singleLockToken = Set.of(lockToken);
        when(mockTimelock.unlock(singleLockToken)).thenReturn(Futures.immediateFuture(singleLockToken));

        return backupToken;
    }

    private BackupToken getInvalidBackupToken() {
        LockToken lockToken = lockToken();
        BackupToken backupToken = backupToken(lockToken);

        when(mockTimelock.unlock(Set.of(lockToken))).thenReturn(Futures.immediateFuture(Set.of()));

        return backupToken;
    }

    private BackupToken backupToken(LockToken lockToken) {
        return BackupToken.builder()
                .namespace(NAMESPACE)
                .lockToken(lockToken)
                .immutableTimestamp(IMMUTABLE_TIMESTAMP)
                .backupStartTimestamp(BACKUP_START_TIMESTAMP)
                .build();
    }

    private LockToken lockToken() {
        UUID requestId = UUID.randomUUID();
        return LockToken.of(requestId);
    }

    // TODO(gs): copied from ConjureTimelockResourceTest
    private static URL url(String url) {
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
