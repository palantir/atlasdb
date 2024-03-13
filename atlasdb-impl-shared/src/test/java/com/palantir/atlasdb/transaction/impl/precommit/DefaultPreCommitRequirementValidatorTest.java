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

package com.palantir.atlasdb.transaction.impl.precommit;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.PreCommitRequirementValidator;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.atlasdb.transaction.impl.ImmutableTimestampLockManager;
import com.palantir.atlasdb.transaction.impl.ImmutableTimestampLockManager.ExpiredLocks;
import com.palantir.atlasdb.transaction.impl.metrics.TransactionOutcomeMetrics;
import com.palantir.lock.v2.LockToken;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class DefaultPreCommitRequirementValidatorTest {
    private static final long TIMESTAMP = 888L;
    private static final RuntimeException RUNTIME_EXCEPTION = new RuntimeException("womp");
    private static final Map<TableReference, Map<Cell, byte[]>> MUTATIONS = ImmutableMap.of(
            TableReference.createFromFullyQualifiedName("keyspace.tables"),
            ImmutableMap.of(
                    Cell.create(PtBytes.toBytes("coffee"), PtBytes.toBytes("dining")),
                    PtBytes.toBytes("one"),
                    Cell.create(PtBytes.toBytes("spreadsheet"), PtBytes.toBytes("pivot")),
                    PtBytes.toBytes("two"),
                    Cell.create(PtBytes.toBytes("suggestion"), PtBytes.toBytes("moot")),
                    PtBytes.toBytes("three")));
    private static final LockToken LOCK_TOKEN = LockToken.of(UUID.randomUUID());

    @Mock
    private PreCommitCondition userPreCommitCondition;

    @Mock
    private TransactionOutcomeMetrics metrics;

    @Mock
    private ImmutableTimestampLockManager immutableTimestampLockManager;

    private PreCommitRequirementValidator validator;

    @BeforeEach
    public void setUp() {
        this.validator = new DefaultPreCommitRequirementValidator(
                userPreCommitCondition, metrics, immutableTimestampLockManager);
    }

    @Test
    public void throwIfPreCommitConditionInvalidPassesTimestampToPreCommitCondition() {
        validator.throwIfPreCommitConditionInvalid(TIMESTAMP);
        verify(userPreCommitCondition).throwIfConditionInvalid(TIMESTAMP);
    }

    @Test
    public void throwIfPreCommitConditionInvalidPassesTimestampPropagatesExceptions() {
        doThrow(RUNTIME_EXCEPTION).when(userPreCommitCondition).throwIfConditionInvalid(anyLong());
        assertThatThrownBy(() -> validator.throwIfPreCommitConditionInvalid(TIMESTAMP))
                .isEqualTo(RUNTIME_EXCEPTION);
    }

    @Test
    public void throwIfPreCommitConditionInvalidAtCommitOnWriteTransactionPassesArgumentsToPreCommitCondition() {
        validator.throwIfPreCommitConditionInvalidAtCommitOnWriteTransaction(MUTATIONS, TIMESTAMP);
        verify(userPreCommitCondition).throwIfConditionInvalid(MUTATIONS, TIMESTAMP);
    }

    @Test
    public void throwIfPreCommitConditionInvalidAtCommitOnWriteTransactionPropagatesExceptions() {
        doThrow(RUNTIME_EXCEPTION).when(userPreCommitCondition).throwIfConditionInvalid(any(), anyLong());
        assertThatThrownBy(() ->
                        validator.throwIfPreCommitConditionInvalidAtCommitOnWriteTransaction(MUTATIONS, TIMESTAMP))
                .isEqualTo(RUNTIME_EXCEPTION);
    }

    @Test
    public void throwIfImmutableTsOrCommitLocksExpiredDelegatesEmptyRequestToLockManager() {
        when(immutableTimestampLockManager.getExpiredImmutableTimestampAndCommitLocks(any()))
                .thenReturn(Optional.empty());
        validator.throwIfImmutableTsOrCommitLocksExpired(null);
        verify(immutableTimestampLockManager).getExpiredImmutableTimestampAndCommitLocks(Optional.empty());
    }

    @Test
    public void throwIfImmutableTsOrCommitLocksExpiredDelegatesRequestWithLockTokenToLockManager() {
        when(immutableTimestampLockManager.getExpiredImmutableTimestampAndCommitLocks(any()))
                .thenReturn(Optional.empty());
        validator.throwIfImmutableTsOrCommitLocksExpired(LOCK_TOKEN);
        verify(immutableTimestampLockManager).getExpiredImmutableTimestampAndCommitLocks(Optional.of(LOCK_TOKEN));
    }

    @Test
    public void throwIfImmutableTsOrCommitLocksExpiredThrowsIfLocksExpired() {
        when(immutableTimestampLockManager.getExpiredImmutableTimestampAndCommitLocks(any()))
                .thenReturn(Optional.of(ExpiredLocks.of(ImmutableSet.of(LOCK_TOKEN), "boo")));
        assertThatThrownBy(() -> validator.throwIfImmutableTsOrCommitLocksExpired(LOCK_TOKEN))
                .isInstanceOf(TransactionFailedException.class);
    }

    @Test
    public void throwIfPreCommitRequirementsNotMetThrowsIfLocksExpired() {
        when(immutableTimestampLockManager.getExpiredImmutableTimestampAndCommitLocks(any()))
                .thenReturn(Optional.of(ExpiredLocks.of(ImmutableSet.of(LOCK_TOKEN), "boo")));
        assertThatThrownBy(() -> validator.throwIfPreCommitRequirementsNotMet(LOCK_TOKEN, TIMESTAMP))
                .isInstanceOf(TransactionFailedException.class);
    }

    @Test
    public void throwIfPreCommitRequirementsNotMetThrowsIfPreCommitConditionThrows() {
        doThrow(RUNTIME_EXCEPTION).when(userPreCommitCondition).throwIfConditionInvalid(anyLong());
        assertThatThrownBy(() -> validator.throwIfPreCommitRequirementsNotMet(LOCK_TOKEN, TIMESTAMP))
                .isEqualTo(RUNTIME_EXCEPTION);
    }

    @Test
    public void throwIfPreCommitRequirementsNotMetDoesNotThrowIfNoLocksExpiredAndPreCommitConditionValid() {
        when(immutableTimestampLockManager.getExpiredImmutableTimestampAndCommitLocks(any()))
                .thenReturn(Optional.empty());

        assertThatCode(() -> validator.throwIfPreCommitRequirementsNotMet(LOCK_TOKEN, TIMESTAMP))
                .doesNotThrowAnyException();
        verify(userPreCommitCondition).throwIfConditionInvalid(TIMESTAMP);
        verify(immutableTimestampLockManager).getExpiredImmutableTimestampAndCommitLocks(Optional.of(LOCK_TOKEN));
    }
}
