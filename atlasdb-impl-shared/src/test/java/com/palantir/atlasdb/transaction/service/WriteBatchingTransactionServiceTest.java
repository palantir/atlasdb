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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.encoding.V1EncodingStrategy;
import com.palantir.common.annotation.Output;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.immutables.value.Value;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WriteBatchingTransactionServiceTest {
    private static final V1EncodingStrategy ENCODING_STRATEGY = V1EncodingStrategy.INSTANCE;

    private final EncodingTransactionService mockTransactionService = mock(EncodingTransactionService.class);
    private final TransactionService writeBatchingTransactionService =
            WriteBatchingTransactionService.create(mockTransactionService);

    @Before
    public void setUp() {
        when(mockTransactionService.getCellEncodingStrategy()).thenReturn(ENCODING_STRATEGY);
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
        WriteBatchingTransactionService.processBatch(
                mockTransactionService,
                ImmutableList.of(
                        TestTransactionBatchElement.of(1L, 100L),
                        TestTransactionBatchElement.of(2L, 200L),
                        TestTransactionBatchElement.of(3L, 300L)));

        verify(mockTransactionService).putUnlessExistsMultiple(ImmutableMap.of(1L, 100L, 2L, 200L, 3L, 300L));
    }

    @Test
    public void filtersOutKeysThatExistOnKeyAlreadyExistsException() {
        KeyAlreadyExistsException keyAlreadyExistsException = new KeyAlreadyExistsException(
                "boo", ImmutableList.of(ENCODING_STRATEGY.encodeStartTimestampAsCell(2L)));
        doThrow(keyAlreadyExistsException)
                .doNothing()
                .when(mockTransactionService)
                .putUnlessExistsMultiple(anyMap());

        TestTransactionBatchElement elementAlreadyExisting = TestTransactionBatchElement.of(2L, 200L);
        TestTransactionBatchElement elementNotExisting = TestTransactionBatchElement.of(3L, 300L);

        WriteBatchingTransactionService.processBatch(
                mockTransactionService, ImmutableList.of(elementAlreadyExisting, elementNotExisting));

        assertThatThrownBy(() -> elementAlreadyExisting.result().get()).hasCause(keyAlreadyExistsException);
        assertThatCode(() -> elementNotExisting.result().get()).doesNotThrowAnyException();

        verify(mockTransactionService).putUnlessExistsMultiple(ImmutableMap.of(2L, 200L, 3L, 300L));
        verify(mockTransactionService).putUnlessExistsMultiple(ImmutableMap.of(3L, 300L));
        verify(mockTransactionService, atLeastOnce()).getCellEncodingStrategy();
    }

    @Test
    public void throwsOnUnspecifiedKeyAlreadyExistsExceptions() {
        KeyAlreadyExistsException keyAlreadyExistsException = new KeyAlreadyExistsException("boo");
        doThrow(keyAlreadyExistsException)
                .doNothing()
                .when(mockTransactionService)
                .putUnlessExistsMultiple(anyMap());

        assertThatThrownBy(() -> WriteBatchingTransactionService.processBatch(
                        mockTransactionService,
                        ImmutableList.of(
                                TestTransactionBatchElement.of(1L, 100L), TestTransactionBatchElement.of(2L, 200L))))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("claimed no keys already existed");

        verify(mockTransactionService, times(1)).putUnlessExistsMultiple(ImmutableMap.of(1L, 100L, 2L, 200L));
    }

    @Test
    public void repeatedProcessBatchDoesNotBreakOnItsOwnSuccessfulWrites() {
        List<Cell> failed = ImmutableList.of(ENCODING_STRATEGY.encodeStartTimestampAsCell(3L));
        List<Cell> succeeded = ImmutableList.of(ENCODING_STRATEGY.encodeStartTimestampAsCell(2L));

        KeyAlreadyExistsException originalException = new KeyAlreadyExistsException("boo", failed, succeeded);

        doThrow(originalException).doNothing().when(mockTransactionService).putUnlessExistsMultiple(anyMap());

        TestTransactionBatchElement elementNotExisting = TestTransactionBatchElement.of(2L, 200L);
        TestTransactionBatchElement elementAlreadyExisting = TestTransactionBatchElement.of(3L, 300L);
        TestTransactionBatchElement elementToRetry = TestTransactionBatchElement.of(4L, 400L);

        WriteBatchingTransactionService.processBatch(
                mockTransactionService, ImmutableList.of(elementNotExisting, elementAlreadyExisting, elementToRetry));

        verify(mockTransactionService).putUnlessExistsMultiple(ImmutableMap.of(2L, 200L, 3L, 300L, 4L, 400L));
        verify(mockTransactionService).putUnlessExistsMultiple(ImmutableMap.of(4L, 400L));

        assertThatThrownBy(() -> elementAlreadyExisting.result().get()).hasCause(originalException);
        assertThatCode(() -> elementNotExisting.result().get()).doesNotThrowAnyException();
        assertThatCode(() -> elementToRetry.result().get()).doesNotThrowAnyException();

        verify(mockTransactionService, atLeastOnce()).getCellEncodingStrategy();
    }

    @Test
    public void repeatedProcessBatchOnlyMakesOneCallWithDuplicatesIfSuccessful() {
        KeyAlreadyExistsException exception = new KeyAlreadyExistsException(
                "boo", ImmutableList.of(ENCODING_STRATEGY.encodeStartTimestampAsCell(5L)));
        doNothing().doThrow(exception).when(mockTransactionService).putUnlessExistsMultiple(anyMap());

        int numRequests = 100;
        List<BatchElement<WriteBatchingTransactionService.TimestampPair, Void>> batchedRequest = IntStream.range(
                        0, numRequests)
                .mapToObj(unused -> TestTransactionBatchElement.of(5L, 9L))
                .collect(Collectors.toList());

        WriteBatchingTransactionService.processBatch(mockTransactionService, batchedRequest);

        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failureCount = new AtomicInteger();
        getResultsTrackingOutcomes(batchedRequest, successCount, failureCount);
        verify(mockTransactionService, times(1)).putUnlessExistsMultiple(anyMap());
        verify(mockTransactionService, atLeastOnce()).getCellEncodingStrategy();

        // XXX Technically invalid, but valid for a mock transaction service.
        assertThat(successCount).hasValue(1);
        assertThat(failureCount).hasValue(numRequests - 1);
    }

    @Test
    public void repeatedProcessBatchOnlyMakesOneCallWithDuplicatesIfFailedAndKnownFailed() {
        KeyAlreadyExistsException exception = new KeyAlreadyExistsException(
                "boo", ImmutableList.of(ENCODING_STRATEGY.encodeStartTimestampAsCell(5L)));
        doThrow(exception).when(mockTransactionService).putUnlessExistsMultiple(anyMap());

        int numRequests = 100;
        List<BatchElement<WriteBatchingTransactionService.TimestampPair, Void>> batchedRequest = IntStream.range(
                        0, numRequests)
                .mapToObj(unused -> TestTransactionBatchElement.of(5L, 9L))
                .collect(Collectors.toList());

        WriteBatchingTransactionService.processBatch(mockTransactionService, batchedRequest);

        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failureCount = new AtomicInteger();
        getResultsTrackingOutcomes(batchedRequest, successCount, failureCount);
        verify(mockTransactionService, times(1)).putUnlessExistsMultiple(anyMap());
        verify(mockTransactionService, atLeastOnce()).getCellEncodingStrategy();

        // XXX Technically invalid, but valid for a mock transaction service.
        assertThat(successCount).hasValue(0);
        assertThat(failureCount).hasValue(numRequests);
    }

    @Test
    public void repeatedProcessBatchFiltersOutPartialSuccesses() {
        KeyAlreadyExistsException exception = new KeyAlreadyExistsException(
                "boo",
                ImmutableList.of(ENCODING_STRATEGY.encodeStartTimestampAsCell(5L)),
                ImmutableList.of(ENCODING_STRATEGY.encodeStartTimestampAsCell(6L)));
        doThrow(exception).when(mockTransactionService).putUnlessExistsMultiple(anyMap());

        int numFailingRequests = 100;
        List<BatchElement<WriteBatchingTransactionService.TimestampPair, Void>> batchedRequest = IntStream.range(
                        0, numFailingRequests)
                .mapToObj(unused -> TestTransactionBatchElement.of(6L, 9L))
                .collect(Collectors.toList());
        batchedRequest.add(TestTransactionBatchElement.of(5L, 888L));

        WriteBatchingTransactionService.processBatch(mockTransactionService, batchedRequest);

        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failureCount = new AtomicInteger();
        getResultsTrackingOutcomes(batchedRequest, successCount, failureCount);
        verify(mockTransactionService, times(1)).putUnlessExistsMultiple(anyMap());
        verify(mockTransactionService, atLeastOnce()).getCellEncodingStrategy();

        // XXX Technically invalid, but valid for a mock transaction service.
        assertThat(successCount).hasValue(1);
        assertThat(failureCount).hasValue(numFailingRequests);
    }

    private static void getResultsTrackingOutcomes(
            List<BatchElement<WriteBatchingTransactionService.TimestampPair, Void>> batchedRequest,
            @Output AtomicInteger successCount,
            @Output AtomicInteger failureCount) {
        batchedRequest.forEach(request -> {
            try {
                request.result().get();
                successCount.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                // OK, we assume this is a KAEE
                failureCount.incrementAndGet();
            }
        });
    }

    @Test
    public void throwsExceptionsCorrectlyOnDuplicatedElementsInBatch() throws InterruptedException {
        EncodingTransactionService encodingTransactionService =
                SimpleTransactionService.createV1(new InMemoryKeyValueService(true));

        int numRequests = 100;
        List<BatchElement<WriteBatchingTransactionService.TimestampPair, Void>> batchedRequest = IntStream.range(
                        0, numRequests)
                .mapToObj(unused -> TestTransactionBatchElement.of(1L, 5L))
                .collect(Collectors.toList());

        WriteBatchingTransactionService.processBatch(encodingTransactionService, batchedRequest);

        AtomicInteger successCounter = new AtomicInteger();
        AtomicInteger exceptionCounter = new AtomicInteger();
        for (BatchElement<WriteBatchingTransactionService.TimestampPair, Void> batchElement : batchedRequest) {
            try {
                batchElement.result().get();
                successCounter.incrementAndGet();
            } catch (ExecutionException ex) {
                assertThat(ex)
                        .hasCauseInstanceOf(KeyAlreadyExistsException.class)
                        .satisfies(executionException -> {
                            KeyAlreadyExistsException keyAlreadyExistsException =
                                    (KeyAlreadyExistsException) executionException.getCause();
                            assertThat(keyAlreadyExistsException.getExistingKeys())
                                    .containsExactly(encodingTransactionService
                                            .getCellEncodingStrategy()
                                            .encodeStartTimestampAsCell(1L));
                        });
                exceptionCounter.incrementAndGet();
            }
        }

        // XXX Not something reasonable to assume in production (since the one successful call might actually
        // return fail while succeeding on the KVS), but acceptable for In Memory KVS.
        assertThat(successCounter).hasValue(1);
        assertThat(exceptionCounter).hasValue(numRequests - 1);
    }

    @SuppressWarnings("immutables:subtype")
    @Value.Immutable
    interface TestTransactionBatchElement extends BatchElement<WriteBatchingTransactionService.TimestampPair, Void> {
        static TestTransactionBatchElement of(long startTimestamp, long commitTimestamp) {
            return ImmutableTestTransactionBatchElement.builder()
                    .argument(ImmutableTimestampPair.of(startTimestamp, commitTimestamp))
                    .result(new DisruptorAutobatcher.DisruptorFuture<>("test"))
                    .build();
        }
    }
}
