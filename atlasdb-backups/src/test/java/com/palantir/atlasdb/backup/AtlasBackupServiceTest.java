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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequest;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockImmutableTimestampResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockToken;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequest;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponse;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequest;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponse;
import com.palantir.atlasdb.timelock.api.NamespacedLockToken;
import com.palantir.atlasdb.timelock.api.PrepareBackupResponse;
import com.palantir.atlasdb.timelock.api.SuccessfulLockImmutableTimestampResponse;
import com.palantir.atlasdb.timelock.api.SuccessfulPrepareBackupResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulLockImmutableTimestampResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulPrepareBackupResponse;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockToken;
import com.palantir.tokens.auth.AuthHeader;
import java.util.Set;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

public class AtlasBackupServiceTest {
    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("header");
    private static final String NAMESPACE = "test";

    private final ConjureTimelockService conjureTimelockService = mock(ConjureTimelockService.class);

    private final AtlasBackupService atlasBackupService = new AtlasBackupService(conjureTimelockService);

    @Test
    public void preparesBackupSuccessfully() {
        UUID requestId = UUID.randomUUID();
        when(conjureTimelockService.lockImmutableTimestamp(any(), any()))
                .thenReturn(ConjureLockImmutableTimestampResponse.successful(
                        SuccessfulLockImmutableTimestampResponse.of(ConjureLockToken.of(requestId), 1L)));

        PrepareBackupResponse response = atlasBackupService.prepareBackup(AUTH_HEADER, NAMESPACE);
        PrepareBackupResponse expected = PrepareBackupResponse.successful(
                SuccessfulPrepareBackupResponse.of(NamespacedLockToken.of(NAMESPACE, LockToken.of(requestId)), 1L));
        assertThat(response).isEqualTo(expected);
    }

    @Test
    public void prepareBackupUnsuccessfulWhenLockImmutableTimestampFails() {
        when(conjureTimelockService.lockImmutableTimestamp(any(), any()))
                .thenReturn(ConjureLockImmutableTimestampResponse.unsuccessful(
                        UnsuccessfulLockImmutableTimestampResponse.of()));

        PrepareBackupResponse response = atlasBackupService.prepareBackup(AUTH_HEADER, NAMESPACE);
        PrepareBackupResponse expected = PrepareBackupResponse.unsuccessful(UnsuccessfulPrepareBackupResponse.of());
        assertThat(response).isEqualTo(expected);
    }

    @Test
    public void canGetTimestampForBackup() {
        when(conjureTimelockService.getFreshTimestamps(
                        eq(AUTH_HEADER), eq(NAMESPACE), eq(ConjureGetFreshTimestampsRequest.of(1))))
                .thenReturn(ConjureGetFreshTimestampsResponse.of(1L, 1L));

        assertThat(atlasBackupService.getFreshTimestampForBackup(AUTH_HEADER, NAMESPACE))
                .isEqualTo(1L);

        // no call to prepare backup
        verify(conjureTimelockService, times(1)).getFreshTimestamps(any(), any(), any());
        verifyNoMoreInteractions(conjureTimelockService);
    }

    @Test
    public void checkBackupIsValidReturnsTrueWhenLockIsHeld() {
        NamespacedLockToken namespacedLockToken = getValidNamespacedLockToken();

        assertThat(atlasBackupService.checkBackupIsValid(AUTH_HEADER, namespacedLockToken))
                .isTrue();
    }

    @Test
    public void checkBackupIsValidReturnsFalseWhenLockIsLost() {
        NamespacedLockToken namespacedLockToken = getInvalidNamespacedLockToken();

        assertThat(atlasBackupService.checkBackupIsValid(AUTH_HEADER, namespacedLockToken))
                .isFalse();
    }

    @Test
    public void completeBackupReturnsTrueWhenLockIsHeld() {
        NamespacedLockToken namespacedLockToken = getValidNamespacedLockToken();

        assertThat(atlasBackupService.completeBackup(AUTH_HEADER, namespacedLockToken))
                .isTrue();
    }

    @Test
    public void completeBackupReturnsTrueWhenLockIsLost() {
        NamespacedLockToken namespacedLockToken = getInvalidNamespacedLockToken();

        assertThat(atlasBackupService.completeBackup(AUTH_HEADER, namespacedLockToken))
                .isFalse();
    }

    @NotNull
    private NamespacedLockToken getValidNamespacedLockToken() {
        UUID requestId = UUID.randomUUID();
        LockToken lockToken = LockToken.of(requestId);
        NamespacedLockToken namespacedLockToken = NamespacedLockToken.of(NAMESPACE, lockToken);

        Set<ConjureLockToken> conjureLockTokens = Set.of(ConjureLockToken.of(requestId));
        Lease mockLease = mock(Lease.class);
        when(conjureTimelockService.refreshLocks(
                        AUTH_HEADER, NAMESPACE, ConjureRefreshLocksRequest.of(conjureLockTokens)))
                .thenReturn(ConjureRefreshLocksResponse.of(conjureLockTokens, mockLease));
        when(conjureTimelockService.unlock(AUTH_HEADER, NAMESPACE, ConjureUnlockRequest.of(conjureLockTokens)))
                .thenReturn(ConjureUnlockResponse.of(conjureLockTokens));

        return namespacedLockToken;
    }

    @NotNull
    private NamespacedLockToken getInvalidNamespacedLockToken() {
        UUID requestId = UUID.randomUUID();
        LockToken lockToken = LockToken.of(requestId);
        NamespacedLockToken namespacedLockToken = NamespacedLockToken.of(NAMESPACE, lockToken);

        Set<ConjureLockToken> conjureLockTokens = Set.of(ConjureLockToken.of(requestId));
        Lease mockLease = mock(Lease.class);
        when(conjureTimelockService.refreshLocks(
                        AUTH_HEADER, NAMESPACE, ConjureRefreshLocksRequest.of(conjureLockTokens)))
                .thenReturn(ConjureRefreshLocksResponse.of(Set.of(), mockLease));
        when(conjureTimelockService.unlock(AUTH_HEADER, NAMESPACE, ConjureUnlockRequest.of(conjureLockTokens)))
                .thenReturn(ConjureUnlockResponse.of(Set.of()));

        return namespacedLockToken;
    }
}
