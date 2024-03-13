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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.transaction.impl.precommit.LockValidityChecker;
import com.palantir.lock.v2.LockToken;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
// TODO (jkong): Reduce duplication of test setup after https://github.com/junit-team/junit5/issues/944 is resolved

public final class ImmutableTimestampLockManagerTest {
    private static final LockToken DEFAULT_IMMUTABLE_TIMESTAMP_LOCK_TOKEN = LockToken.of(UUID.randomUUID());
    private static final LockToken DEFAULT_COMMIT_LOCK_TOKEN = LockToken.of(UUID.randomUUID());

    @ParameterizedTest(name = "{2}")
    @MethodSource("lockManagerConfigurations")
    public void returnsNothingExpiredWhenAllLocksStillValid(
            Optional<LockToken> immutableTimestampLock, Optional<LockToken> userCommitLock, String testDescription) {
        LockValidityChecker validityChecker = mock(LockValidityChecker.class);
        ImmutableTimestampLockManager immutableTimestampLockManager =
                new ImmutableTimestampLockManager(immutableTimestampLock, validityChecker);

        when(validityChecker.getStillValidLockTokens(anySet()))
                .thenAnswer(invocation -> invocation.getArguments()[0]);

        assertThat(immutableTimestampLockManager.getExpiredImmutableTimestampAndCommitLocks(userCommitLock))
                .isEmpty();
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("lockManagerConfigurationsWithPresentUserCommitLock")
    public void returnsLocksThatWereCheckedWhenGettingFullSummaryAndStillValid(
            Optional<LockToken> immutableTimestampLock, String testDescription) {
        LockValidityChecker validityChecker = mock(LockValidityChecker.class);
        ImmutableTimestampLockManager immutableTimestampLockManager =
                new ImmutableTimestampLockManager(immutableTimestampLock, validityChecker);

        when(validityChecker.getStillValidLockTokens(anySet()))
                .thenAnswer(invocation -> invocation.getArguments()[0]);

        assertThat(immutableTimestampLockManager.getExpiredImmutableTimestampAndCommitLocksWithFullSummary(
                        DEFAULT_COMMIT_LOCK_TOKEN))
                .satisfies(summarizedLockCheckResult -> {
                    assertThat(summarizedLockCheckResult.expiredLocks()).isEmpty();
                    assertThat(summarizedLockCheckResult.immutableTimestampLock())
                            .isEqualTo(immutableTimestampLock);
                    assertThat(summarizedLockCheckResult.userProvidedLock()).isEqualTo(DEFAULT_COMMIT_LOCK_TOKEN);
                });
    }

    @ParameterizedTest(name = "{2}")
    @MethodSource("lockManagerConfigurations")
    public void returnsExpiredLocksWhenLocksNoLongerValid(
            Optional<LockToken> immutableTimestampLock, Optional<LockToken> userCommitLock, String testDescription) {
        Set<LockToken> expectedLocks = Stream.of(immutableTimestampLock, userCommitLock)
                .flatMap(Optional::stream)
                .collect(Collectors.toSet());

        Assumptions.assumeTrue(
                immutableTimestampLock.isPresent() || userCommitLock.isPresent(),
                "Test not significant if both locks are not present");

        LockValidityChecker validityChecker = mock(LockValidityChecker.class);
        ImmutableTimestampLockManager immutableTimestampLockManager =
                new ImmutableTimestampLockManager(immutableTimestampLock, validityChecker);

        when(validityChecker.getStillValidLockTokens(anySet())).thenReturn(ImmutableSet.of());

        assertThat(immutableTimestampLockManager.getExpiredImmutableTimestampAndCommitLocks(userCommitLock))
                .hasValueSatisfying(expiredLocks -> {
                    assertThat(expiredLocks.expiredLockTokens()).isEqualTo(expectedLocks);
                    assertThat(expiredLocks.errorString()).contains(immutableTimestampLock.toString());
                    assertThat(expiredLocks.errorString()).contains(userCommitLock.toString());
                });
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("lockManagerConfigurationsWithPresentUserCommitLock")
    public void returnsLocksThatWereCheckedWhenGettingFullSummaryAndExpired(
            Optional<LockToken> immutableTimestampLock, String testDescription) {
        Set<LockToken> expectedLocks = new HashSet<>();
        immutableTimestampLock.ifPresent(expectedLocks::add);
        expectedLocks.add(DEFAULT_COMMIT_LOCK_TOKEN);

        LockValidityChecker validityChecker = mock(LockValidityChecker.class);
        ImmutableTimestampLockManager immutableTimestampLockManager =
                new ImmutableTimestampLockManager(immutableTimestampLock, validityChecker);

        when(validityChecker.getStillValidLockTokens(anySet())).thenReturn(ImmutableSet.of());

        assertThat(immutableTimestampLockManager.getExpiredImmutableTimestampAndCommitLocksWithFullSummary(
                        DEFAULT_COMMIT_LOCK_TOKEN))
                .satisfies(summarizedLockCheckResult -> {
                    assertThat(summarizedLockCheckResult.expiredLocks()).hasValueSatisfying(expiredLocks -> {
                        assertThat(expiredLocks.expiredLockTokens()).isEqualTo(expectedLocks);
                    });
                    assertThat(summarizedLockCheckResult.immutableTimestampLock())
                            .isEqualTo(immutableTimestampLock);
                    assertThat(summarizedLockCheckResult.userProvidedLock()).isEqualTo(DEFAULT_COMMIT_LOCK_TOKEN);
                });
    }

    @Test
    public void throwsIfOnlyCommitLockExpiredWhenCheckingBoth() {
        LockValidityChecker validityChecker = mock(LockValidityChecker.class);
        ImmutableTimestampLockManager immutableTimestampLockManager =
                new ImmutableTimestampLockManager(Optional.of(DEFAULT_IMMUTABLE_TIMESTAMP_LOCK_TOKEN), validityChecker);

        when(validityChecker.getStillValidLockTokens(anySet()))
                .thenReturn(ImmutableSet.of(DEFAULT_IMMUTABLE_TIMESTAMP_LOCK_TOKEN));

        assertThat(immutableTimestampLockManager.getExpiredImmutableTimestampAndCommitLocks(
                        Optional.of(DEFAULT_COMMIT_LOCK_TOKEN)))
                .hasValueSatisfying(expiredLocks -> {
                    assertThat(expiredLocks.expiredLockTokens()).contains(DEFAULT_COMMIT_LOCK_TOKEN);
                    // This is a bit fragile, but emphasising readability here
                    assertThat(expiredLocks.errorString())
                            .contains("the following locks are no longer valid: [" + DEFAULT_COMMIT_LOCK_TOKEN + "]");
                });
    }

    @Test
    public void throwsIfOnlyImmutableTimestampLockExpiredWhenCheckingBoth() {
        LockValidityChecker validityChecker = mock(LockValidityChecker.class);
        ImmutableTimestampLockManager immutableTimestampLockManager =
                new ImmutableTimestampLockManager(Optional.of(DEFAULT_IMMUTABLE_TIMESTAMP_LOCK_TOKEN), validityChecker);

        when(validityChecker.getStillValidLockTokens(anySet())).thenReturn(ImmutableSet.of(DEFAULT_COMMIT_LOCK_TOKEN));

        assertThat(immutableTimestampLockManager.getExpiredImmutableTimestampAndCommitLocks(
                        Optional.of(DEFAULT_COMMIT_LOCK_TOKEN)))
                .hasValueSatisfying(expiredLocks -> {
                    assertThat(expiredLocks.expiredLockTokens()).contains(DEFAULT_IMMUTABLE_TIMESTAMP_LOCK_TOKEN);
                    // This is a bit fragile, but emphasising readability here
                    assertThat(expiredLocks.errorString())
                            .contains("the following locks are no longer valid: ["
                                    + DEFAULT_IMMUTABLE_TIMESTAMP_LOCK_TOKEN + "]");
                });
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
