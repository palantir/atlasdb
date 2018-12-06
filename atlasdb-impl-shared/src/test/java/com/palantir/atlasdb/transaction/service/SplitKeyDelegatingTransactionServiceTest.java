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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.function.Function;

import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class SplitKeyDelegatingTransactionServiceTest {
    private static final Function<Long, Long> EXTRACT_LAST_DIGIT = num -> num % 10;

    private final TransactionService delegate1 = mock(TransactionService.class); // using 0 for modulo reasons
    private final TransactionService delegate2 = mock(TransactionService.class);
    private final TransactionService delegate3 = mock(TransactionService.class);

    private final Map<Long, TransactionService> transactionServiceMap = ImmutableMap.of(
            1L, delegate1, 2L, delegate2, 3L, delegate3);
    private final TransactionService delegatingTransactionService
            = new SplitKeyDelegatingTransactionService<>(EXTRACT_LAST_DIGIT, transactionServiceMap);

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
        assertThatThrownBy(() -> delegatingTransactionService.get(7L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Could not find a transaction service for timestamp {}");
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
        assertThatThrownBy(() -> delegatingTransactionService.putUnlessExists(4L, 12L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Could not find a transaction service for timestamp {}");
    }

    @Test
    public void getMultipleDelegatesRequestsToGetMultipleOnDelegates() {
        when(delegate1.get(eq(ImmutableList.of(1L)))).thenReturn(ImmutableMap.of(1L, 2L));
        assertThat(delegatingTransactionService.get(ImmutableList.of(1L)))
                .isEqualTo(ImmutableMap.of(1L, 2L));
        verify(delegate1).get(eq(ImmutableList.of(1L)));
    }

    @Test
    public void getMultiplePartitionsRequestsAndMergesMaps() {
        when(delegate1.get(eq(ImmutableList.of(1L, 41L)))).thenReturn(ImmutableMap.of(1L, 8L, 41L, 48L));
        when(delegate2.get(eq(ImmutableList.of(12L, 32L)))).thenReturn(ImmutableMap.of(12L, 28L, 32L, 38L));

        assertThat(delegatingTransactionService.get(ImmutableList.of(1L, 12L, 32L, 41L)))
                .isEqualTo(ImmutableMap.of(1L, 8L, 12L, 28L, 32L, 38L, 41L, 48L));
        verify(delegate1).get(eq(ImmutableList.of(1L, 41L)));
        verify(delegate2).get(eq(ImmutableList.of(12L, 32L)));
    }

    @Test
    public void getMultipleThrowsAndDoesNotMakeRequestsIfAnyTimestampsCannotBeMapped() {
        when(delegate1.get(eq(ImmutableList.of(1L, 41L)))).thenReturn(ImmutableMap.of(1L, 8L, 41L, 48L));
        assertThatThrownBy(() -> delegatingTransactionService.get(ImmutableList.of(1L, 7L, 41L)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("A batch of timestamps {} produced some transaction service keys which are"
                        + " unknown");

        // no requests made is implicit, since delegate1 is a mock
    }
}
