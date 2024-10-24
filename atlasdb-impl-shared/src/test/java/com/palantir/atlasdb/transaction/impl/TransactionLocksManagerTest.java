/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.transaction.impl.TransactionLocksManager.LockUnlocker;
import com.palantir.atlasdb.transaction.impl.precommit.LockValidityChecker;
import com.palantir.lock.v2.LockToken;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

// TODO (jkong): Reduce duplication of test setup after https://github.com/junit-team/junit5/issues/944 is resolved
@ExtendWith(MockitoExtension.class)
public final class TransactionLocksManagerTest {
    private static final LockToken DEFAULT_IMMUTABLE_TIMESTAMP_LOCK_TOKEN = LockToken.of(UUID.randomUUID());
    private static final LockToken DEFAULT_COMMIT_LOCK_TOKEN = LockToken.of(UUID.randomUUID());

    @Mock
    private LockUnlocker unlocker;

    @Mock
    private LockValidityChecker validityChecker;

    @ParameterizedTest(name = "{2}")
    @MethodSource("lockManagerConfigurations")
    public void returnsNothingExpiredWhenAllLocksStillValid(
            Optional<LockToken> immutableTimestampLock, Optional<LockToken> userCommitLock, String testDescription) {
        TransactionLocksManager transactionLocksManager =
                new TransactionLocksManager(immutableTimestampLock, validityChecker, unlocker);

        if (immutableTimestampLock.isPresent() || userCommitLock.isPresent()) {
            when(validityChecker.getStillValidLockTokens(anySet()))
                    .thenAnswer(invocation -> invocation.getArguments()[0]);
        }
        userCommitLock.ifPresent(transactionLocksManager::registerLock);

        assertThat(transactionLocksManager.getExpiredImmutableTimestampAndCommitLocks())
                .isEmpty();
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("lockManagerConfigurationsWithPresentUserCommitLock")
    public void returnsLocksThatWereCheckedWhenGettingFullSummaryAndStillValid(
            Optional<LockToken> immutableTimestampLock, String testDescription) {
        TransactionLocksManager transactionLocksManager =
                new TransactionLocksManager(immutableTimestampLock, validityChecker, unlocker);

        when(validityChecker.getStillValidLockTokens(anySet()))
                .thenAnswer(invocation -> invocation.getArguments()[0]);

        transactionLocksManager.registerCommitLockOnly(DEFAULT_COMMIT_LOCK_TOKEN);

        assertThat(transactionLocksManager.getExpiredImmutableTimestampAndCommitLocksWithFullSummary())
                .satisfies(summarizedLockCheckResult -> {
                    assertThat(summarizedLockCheckResult.expiredLocks()).isEmpty();
                    assertThat(summarizedLockCheckResult.immutableTimestampLock())
                            .isEqualTo(immutableTimestampLock);
                    assertThat(summarizedLockCheckResult.allLockTokens())
                            .containsExactlyInAnyOrderElementsOf(
                                    Stream.concat(immutableTimestampLock.stream(), Stream.of(DEFAULT_COMMIT_LOCK_TOKEN))
                                            .collect(Collectors.toSet()));
                });
    }

    @ParameterizedTest(name = "{2}")
    @MethodSource("lockManagerConfigurations")
    public void returnsExpiredLocksWhenLocksNoLongerValid(
            Optional<LockToken> immutableTimestampLock, Optional<LockToken> userCommitLock, String testDescription) {
        Assumptions.assumeTrue(
                immutableTimestampLock.isPresent() || userCommitLock.isPresent(),
                "Test not significant if both locks are not present");

        TransactionLocksManager transactionLocksManager =
                new TransactionLocksManager(immutableTimestampLock, validityChecker, unlocker);

        when(validityChecker.getStillValidLockTokens(anySet())).thenReturn(ImmutableSet.of());

        userCommitLock.ifPresent(transactionLocksManager::registerLock);
        assertThat(transactionLocksManager.getExpiredImmutableTimestampAndCommitLocks())
                .hasValueSatisfying(expiredLocks -> {
                    assertThat(expiredLocks.errorDescription()).contains(immutableTimestampLock.toString());
                    userCommitLock.ifPresent(lockToken ->
                            assertThat(expiredLocks.errorDescription()).contains(lockToken.toString()));
                });
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("lockManagerConfigurationsWithPresentUserCommitLock")
    public void returnsLocksThatWereCheckedWhenGettingFullSummaryAndExpired(
            Optional<LockToken> immutableTimestampLock, String testDescription) {
        TransactionLocksManager transactionLocksManager =
                new TransactionLocksManager(immutableTimestampLock, validityChecker, unlocker);

        when(validityChecker.getStillValidLockTokens(anySet())).thenReturn(ImmutableSet.of());

        transactionLocksManager.registerCommitLockOnly(DEFAULT_COMMIT_LOCK_TOKEN);
        assertThat(transactionLocksManager.getExpiredImmutableTimestampAndCommitLocksWithFullSummary())
                .satisfies(summarizedLockCheckResult -> {
                    assertThat(summarizedLockCheckResult.expiredLocks()).isPresent();
                    assertThat(summarizedLockCheckResult.immutableTimestampLock())
                            .isEqualTo(immutableTimestampLock);
                    assertThat(summarizedLockCheckResult.allLockTokens())
                            .containsExactlyInAnyOrderElementsOf(
                                    Stream.concat(immutableTimestampLock.stream(), Stream.of(DEFAULT_COMMIT_LOCK_TOKEN))
                                            .collect(Collectors.toSet()));
                });
    }

    @Test
    public void throwsIfOnlyCommitLockExpiredWhenCheckingBoth() {
        TransactionLocksManager transactionLocksManager = new TransactionLocksManager(
                Optional.of(DEFAULT_IMMUTABLE_TIMESTAMP_LOCK_TOKEN), validityChecker, unlocker);

        when(validityChecker.getStillValidLockTokens(anySet()))
                .thenReturn(ImmutableSet.of(DEFAULT_IMMUTABLE_TIMESTAMP_LOCK_TOKEN));

        transactionLocksManager.registerLock(DEFAULT_COMMIT_LOCK_TOKEN);
        assertThat(transactionLocksManager.getExpiredImmutableTimestampAndCommitLocks())
                .hasValueSatisfying(expiredLocks -> {
                    // This is a bit fragile, but emphasising readability here
                    assertThat(expiredLocks.errorDescription())
                            .contains("the following locks are no longer valid: [" + DEFAULT_COMMIT_LOCK_TOKEN + "]");
                });
    }

    @Test
    public void throwsIfOnlyImmutableTimestampLockExpiredWhenCheckingBoth() {
        TransactionLocksManager transactionLocksManager = new TransactionLocksManager(
                Optional.of(DEFAULT_IMMUTABLE_TIMESTAMP_LOCK_TOKEN), validityChecker, unlocker);

        when(validityChecker.getStillValidLockTokens(anySet())).thenReturn(ImmutableSet.of(DEFAULT_COMMIT_LOCK_TOKEN));

        transactionLocksManager.registerLock(DEFAULT_COMMIT_LOCK_TOKEN);
        assertThat(transactionLocksManager.getExpiredImmutableTimestampAndCommitLocks())
                .hasValueSatisfying(expiredLocks -> {
                    // This is a bit fragile, but emphasising readability here
                    assertThat(expiredLocks.errorDescription())
                            .contains("the following locks are no longer valid: ["
                                    + DEFAULT_IMMUTABLE_TIMESTAMP_LOCK_TOKEN + "]");
                });
    }

    @Test
    public void doesNotCallLockRefresherIfNothingToCheck() {
        LockValidityChecker validityChecker = mock(LockValidityChecker.class);
        TransactionLocksManager transactionLocksManager =
                new TransactionLocksManager(Optional.empty(), validityChecker, unlocker);

        assertThat(transactionLocksManager.getExpiredImmutableTimestampAndCommitLocks())
                .isEmpty();
        verify(validityChecker, never()).getStillValidLockTokens(anySet());
    }

    @Test
    public void doesNotCallUnlockIfNoLocksToUnlock() {
        TransactionLocksManager transactionLocksManager =
                new TransactionLocksManager(Optional.empty(), validityChecker, unlocker);

        transactionLocksManager.close();
        verify(unlocker, never()).tryUnlock(anySet());
    }

    @Test
    public void unlocksImmutableTimestampLockOnClose() {
        TransactionLocksManager transactionLocksManager = new TransactionLocksManager(
                Optional.of(DEFAULT_IMMUTABLE_TIMESTAMP_LOCK_TOKEN), validityChecker, unlocker);

        transactionLocksManager.close();
        verify(unlocker).tryUnlock(assertArg(set -> assertThat(set)
                .containsExactlyInAnyOrder(DEFAULT_IMMUTABLE_TIMESTAMP_LOCK_TOKEN)));
    }

    @Test
    public void unlocksAllLocksOnClose() {
        TransactionLocksManager transactionLocksManager = new TransactionLocksManager(
                Optional.of(DEFAULT_IMMUTABLE_TIMESTAMP_LOCK_TOKEN), validityChecker, unlocker);

        transactionLocksManager.registerLock(DEFAULT_COMMIT_LOCK_TOKEN);
        transactionLocksManager.close();
        verify(unlocker).tryUnlock(assertArg(set -> assertThat(set)
                .containsExactlyInAnyOrder(DEFAULT_COMMIT_LOCK_TOKEN, DEFAULT_IMMUTABLE_TIMESTAMP_LOCK_TOKEN)));
    }

    private static Stream<Arguments> lockManagerConfigurations() {
        return Stream.of(
                Arguments.of(Optional.empty(), Optional.empty(), "no immutable timestamp lock and no user commit lock"),
                Arguments.of(Optional.empty(), Optional.of(DEFAULT_COMMIT_LOCK_TOKEN), "only user commit lock"),
                Arguments.of(
                        Optional.of(DEFAULT_IMMUTABLE_TIMESTAMP_LOCK_TOKEN),
                        Optional.empty(),
                        "only immutable timestamp lock"),
                Arguments.of(
                        Optional.of(DEFAULT_IMMUTABLE_TIMESTAMP_LOCK_TOKEN),
                        Optional.of(DEFAULT_COMMIT_LOCK_TOKEN),
                        "both immutable timestamp lock and user commit lock"));
    }

    private static Stream<Arguments> lockManagerConfigurationsWithPresentUserCommitLock() {
        return Stream.of(
                Arguments.of(Optional.empty(), "only user commit lock"),
                Arguments.of(
                        Optional.of(DEFAULT_IMMUTABLE_TIMESTAMP_LOCK_TOKEN),
                        "both immutable timestamp lock and user commit lock"));
    }
}
