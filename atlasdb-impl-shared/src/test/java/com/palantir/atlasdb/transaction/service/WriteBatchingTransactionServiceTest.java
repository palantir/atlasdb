/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.immutables.value.Value;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.transaction.encoding.V1EncodingStrategy;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

public class WriteBatchingTransactionServiceTest {
    private static final V1EncodingStrategy ENCODING_STRATEGY = V1EncodingStrategy.INSTANCE;

    private final EncodingTransactionService mockTransactionService = mock(EncodingTransactionService.class);
    private final TransactionService writeBatchingTransactionService = WriteBatchingTransactionService.create(
            mockTransactionService);

    @Before
    public void setUp() {
        when(mockTransactionService.getEncodingStrategy()).thenReturn(ENCODING_STRATEGY);
    }

    @After
    public void verifyMocks() {
        verifyNoMoreInteractions(mockTransactionService);
    }

    @Test
    public void getsValuesFromUnderlying() {
        when(mockTransactionService.get(anyLong())).thenReturn(5L);

        assertThat(writeBatchingTransactionService.get(3L)).isEqualTo(5L);

        verify(mockTransactionService).get(3L);
    }

    @Test
    public void putsUnlessExistsToUnderlyingViaBatch() {
        writeBatchingTransactionService.putUnlessExists(7, 66);

        verify(mockTransactionService).putUnlessExistsMultiple(ImmutableMap.of(7L, 66L));
    }

    @Test
    public void batchesElementsAndDelegates() {
        WriteBatchingTransactionService.processBatch(mockTransactionService, ImmutableList.of(
                TestTransactionBatchElement.of(1L, 100L),
                TestTransactionBatchElement.of(2L, 200L),
                TestTransactionBatchElement.of(3L, 300L)));

        verify(mockTransactionService).putUnlessExistsMultiple(
                ImmutableMap.of(1L, 100L, 2L, 200L, 3L, 300L));
    }

    @Test
    public void filtersOutKeysThatExistOnKeyAlreadyExistsException() {
        KeyAlreadyExistsException keyAlreadyExistsException = new KeyAlreadyExistsException("boo", ImmutableList.of(
                ENCODING_STRATEGY.encodeStartTimestampAsCell(2L)));
        doThrow(keyAlreadyExistsException)
                .doNothing()
                .when(mockTransactionService)
                .putUnlessExistsMultiple(anyMap());

        TestTransactionBatchElement elementAlreadyExisting = TestTransactionBatchElement.of(2L, 200L);
        TestTransactionBatchElement elementNotExisting = TestTransactionBatchElement.of(3L, 300L);

        WriteBatchingTransactionService.processBatch(mockTransactionService, ImmutableList.of(
                elementAlreadyExisting, elementNotExisting));

        assertThatThrownBy(() -> elementAlreadyExisting.result().get()).hasCause(keyAlreadyExistsException);
        assertThatCode(() -> elementNotExisting.result().get()).doesNotThrowAnyException();

        verify(mockTransactionService).putUnlessExistsMultiple(ImmutableMap.of(2L, 200L, 3L, 300L));
        verify(mockTransactionService).putUnlessExistsMultiple(ImmutableMap.of(3L, 300L));
        verify(mockTransactionService).getEncodingStrategy();
    }

    @Test
    public void throwsOnUnspecifiedKeyAlreadyExistsExceptions() {
        KeyAlreadyExistsException keyAlreadyExistsException = new KeyAlreadyExistsException("boo");
        doThrow(keyAlreadyExistsException)
                .doNothing()
                .when(mockTransactionService)
                .putUnlessExistsMultiple(anyMap());

        assertThatThrownBy(() ->
                WriteBatchingTransactionService.processBatch(mockTransactionService, ImmutableList.of(
                        TestTransactionBatchElement.of(1L, 100L),
                        TestTransactionBatchElement.of(2L, 200L))))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("claimed no keys already existed");

        verify(mockTransactionService, times(1)).putUnlessExistsMultiple(ImmutableMap.of(1L, 100L, 2L, 200L));
    }

    @Test
    public void repeatedProcessBatchDoesNotBreakOnItsOwnSuccessfulWrites() {
        KeyAlreadyExistsException originalException = new KeyAlreadyExistsException("boo", ImmutableList.of(
                ENCODING_STRATEGY.encodeStartTimestampAsCell(3L)));
        KeyAlreadyExistsException newExistingKeyException = new KeyAlreadyExistsException("boo", ImmutableList.of(
                ENCODING_STRATEGY.encodeStartTimestampAsCell(2L)));

        doThrow(originalException).doThrow(originalException)
                .doThrow(newExistingKeyException)
                .doNothing()
                .when(mockTransactionService)
                .putUnlessExistsMultiple(anyMap());

        TestTransactionBatchElement elementNotExisting = TestTransactionBatchElement.of(2L, 200L);
        TestTransactionBatchElement elementAlreadyExisting = TestTransactionBatchElement.of(3L, 300L);

        WriteBatchingTransactionService.processBatch(mockTransactionService, ImmutableList.of(
                elementNotExisting, elementAlreadyExisting));

        verify(mockTransactionService).putUnlessExistsMultiple(ImmutableMap.of(2L, 200L, 3L, 300L));

        assertThatThrownBy(() -> elementAlreadyExisting.result().get()).hasCause(originalException);
        assertThatCode(() -> elementNotExisting.result().get()).doesNotThrowAnyException();

        verify(mockTransactionService).getEncodingStrategy();
    }

    @Test
    public void filtersOutDuplicateKeysInBatcher() {
        TestTransactionBatchElement firstElement = TestTransactionBatchElement.of(1L, 200L);
        TestTransactionBatchElement secondElement = TestTransactionBatchElement.of(1L, 300L);

        WriteBatchingTransactionService.processBatch(mockTransactionService, ImmutableList.of(
                firstElement, secondElement));

        assertThatCode(() -> firstElement.result().get()).doesNotThrowAnyException();
        assertThatThrownBy(() -> secondElement.result().get()).hasCauseInstanceOf(SafeIllegalArgumentException.class);

        verify(mockTransactionService).putUnlessExistsMultiple(ImmutableMap.of(1L, 200L));
    }

    @Value.Immutable
    interface TestTransactionBatchElement extends BatchElement<WriteBatchingTransactionService.TimestampPair, Void> {
        static TestTransactionBatchElement of(long startTimestamp, long commitTimestamp) {
            return ImmutableTestTransactionBatchElement.builder()
                    .argument(ImmutableTimestampPair.of(startTimestamp, commitTimestamp))
                    .result(SettableFuture.create())
                    .build();
        }
    }
}
