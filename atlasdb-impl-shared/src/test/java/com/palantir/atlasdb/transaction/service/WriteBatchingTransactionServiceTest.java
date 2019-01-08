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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.immutables.value.Value;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.atlasdb.autobatch.BatchElement;

public class WriteBatchingTransactionServiceTest {
    private final TransactionService mockTransactionService = mock(TransactionService.class);

    @Test
    public void batchesElementsAndDelegates() {
        WriteBatchingTransactionService.processBatch(mockTransactionService, ImmutableList.of(
                TestTransactionBatchElement.of(1L, 100L),
                TestTransactionBatchElement.of(2L, 200L),
                TestTransactionBatchElement.of(3L, 300L)));

        verify(mockTransactionService).putUnlessExistsMultiple(
                ImmutableMap.of(1L, 100L, 2L, 200L, 3L, 300L));
        verifyNoMoreInteractions(mockTransactionService);
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
