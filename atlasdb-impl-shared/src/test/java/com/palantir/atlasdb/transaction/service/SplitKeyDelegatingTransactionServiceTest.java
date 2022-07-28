/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.service;

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.math.LongMath;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Map;
import java.util.function.Function;
import org.junit.After;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@SuppressWarnings("unchecked") // Mocking
public class SplitKeyDelegatingTransactionServiceTest {
    private static final Function<Long, Long> EXTRACT_LAST_DIGIT = num -> LongMath.mod(num, 10L);
    private static final String NOT_FOUND_MESSAGE =
            "Could not find a transaction service for the given timestamp (serviceKey not found).";

    private final TransactionService delegate1 = mock(TransactionService.class);
    private final TransactionService delegate2 = mock(TransactionService.class);
    private final TransactionService delegate3 = mock(TransactionService.class);

    private final Map<Long, TransactionService> transactionServiceMap =
            ImmutableMap.of(1L, delegate1, 2L, delegate2, 3L, delegate3);
    private final TransactionService delegatingTransactionService =
            new SplitKeyDelegatingTransactionService<>(EXTRACT_LAST_DIGIT, transactionServiceMap);
    private final TransactionService lastDigitFiveImpliesUnknownTransactionService =
            new SplitKeyDelegatingTransactionService<>(num -> num % 10 == 5 ? null : num % 10, transactionServiceMap);

    @After
    public void verifyNoMoreInteractions() {
        Mockito.verifyNoMoreInteractions(delegate1, delegate2, delegate3);
    }

    @Test
    public void canCallGetOnDelegate() {
        when(delegate1.get(1L)).thenReturn(2L);
        assertThat(delegatingTransactionService.get(1L)).isEqualTo(2L);
        verify(delegate1).get(1L);
    }

    @Test
    public void delegatesGetBasedOnFunction() {
        when(delegate1.get(1L)).thenReturn(2L);
        when(delegate2.get(2L)).thenReturn(4L);
        assertThat(delegatingTransactionService.get(1L)).isEqualTo(2L);
        assertThat(delegatingTransactionService.get(2L)).isEqualTo(4L);
        verify(delegate1).get(1L);
        verify(delegate2).get(2L);
    }

    @Test
    public void getThrowsIfFunctionReturnsUnmappedValue() {
        assertThatLoggableExceptionThrownBy(() -> delegatingTransactionService.get(7L))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasLogMessage(NOT_FOUND_MESSAGE);
    }

    @Test
    public void rethrowsExceptionsFromMappingFunction() {
        RuntimeException ex = new IllegalStateException("bad");
        TransactionService unusableService = new SplitKeyDelegatingTransactionService<>(
                num -> {
                    throw ex;
                },
                transactionServiceMap);

        assertThatThrownBy(() -> unusableService.get(5L)).isEqualTo(ex);
    }

    @Test
    public void putUnlessExistsDelegateDecidedByStartTimestamp() {
        delegatingTransactionService.putUnlessExists(3L, 12L);
        verify(delegate3).putUnlessExists(3L, 12L);
    }

    @Test
    public void putUnlessExistsThrowsIfFunctionReturnsUnmappedValue() {
        assertThatLoggableExceptionThrownBy(() -> delegatingTransactionService.putUnlessExists(4L, 12L))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasLogMessage(NOT_FOUND_MESSAGE);
    }

    @Test
    public void getMultipleDelegatesRequestsToGetMultipleOnDelegates() {
        when(delegate1.get(any())).thenReturn(ImmutableMap.of(1L, 2L));
        assertThat(delegatingTransactionService.get(ImmutableList.of(1L)))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(1L, 2L));
        verifyDelegateHadMultigetCalledWith(delegate1, 1L);
    }

    @Test
    public void getMultiplePartitionsRequestsAndMergesMaps() {
        when(delegate1.get(any())).thenReturn(ImmutableMap.of(1L, 8L, 41L, 48L));
        when(delegate2.get(any())).thenReturn(ImmutableMap.of(12L, 28L, 32L, 38L));
        assertThat(delegatingTransactionService.get(ImmutableList.of(1L, 12L, 32L, 41L)))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(1L, 8L, 12L, 28L, 32L, 38L, 41L, 48L));
        verifyDelegateHadMultigetCalledWith(delegate1, 1L, 41L);
        verifyDelegateHadMultigetCalledWith(delegate2, 12L, 32L);
    }

    @Test
    public void getMultipleThrowsAndDoesNotMakeRequestsIfAnyTimestampsCannotBeMapped() {
        when(delegate1.get(any())).thenReturn(ImmutableMap.of(1L, 8L, 41L, 48L));
        assertThatLoggableExceptionThrownBy(() -> delegatingTransactionService.get(ImmutableList.of(1L, 7L, 41L)))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasLogMessage("A batch of timestamps produced some transaction service keys which are unknown.");

        Mockito.verifyNoMoreInteractions(delegate1);
    }

    @Test
    public void ignoreUnknownIgnoresUnknownTimestampServicesForSingleTimestamps() {
        assertThat(lastDigitFiveImpliesUnknownTransactionService.get(5L)).isNull();
    }

    @Test
    public void ignoreUnknownIgnoresUnknownTimestampServicesForMultipleTimestamps() {
        when(delegate1.get(any())).thenReturn(ImmutableMap.of(1L, 8L, 41L, 48L));

        assertThat(lastDigitFiveImpliesUnknownTransactionService.get(ImmutableList.of(1L, 5L, 35L, 41L)))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(1L, 8L, 41L, 48L));
        verifyDelegateHadMultigetCalledWith(delegate1, 1L, 41L);
    }

    @Test
    public void ignoreUnknownFailsIfSeeingATimestampServiceItDoesNotRecognizeForSingleTimestamps() {
        assertThatLoggableExceptionThrownBy(() -> lastDigitFiveImpliesUnknownTransactionService.get(7L))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasLogMessage(NOT_FOUND_MESSAGE);
    }

    @Test
    public void ignoreUnknownFailsIfSeeingATimestampServiceItDoesNotRecognizeForMultipleTimestamps() {
        when(delegate1.get(any())).thenReturn(ImmutableMap.of(1L, 8L, 41L, 48L));
        assertThatLoggableExceptionThrownBy(
                        () -> lastDigitFiveImpliesUnknownTransactionService.get(ImmutableList.of(1L, 5L, 7L, 41L)))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("A batch of timestamps produced some transaction service keys which are unknown");

        Mockito.verifyNoMoreInteractions(delegate1);
    }

    private void verifyDelegateHadMultigetCalledWith(TransactionService delegate, Long... startTimestamps) {
        ArgumentCaptor<Iterable<Long>> captor = ArgumentCaptor.forClass(Iterable.class);
        verify(delegate).get(captor.capture());
        assertThat(captor.getValue()).contains(startTimestamps);
    }
}
