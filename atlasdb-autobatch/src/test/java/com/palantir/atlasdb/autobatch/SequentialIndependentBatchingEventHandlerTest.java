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

package com.palantir.atlasdb.autobatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class SequentialIndependentBatchingEventHandlerTest {
    private static final long IGNORED_SEQUENCE = 0L;

    // hand-rolling a stub because using Mockito for a consumer of a list
    // yields counter-intuitive behaviour, as Mockito will capture the list
    // reference whose contents may be mutated by the handler
    private final StubbedBatchFunction batchFunction = new StubbedBatchFunction();

    private final AutobatcherEventHandler<String, String> handler =
            IndependentBatchingEventHandler.createWithSequentialBatchProcessing(batchFunction, 16);

    @Test
    public void handlerCloseClosesPool() throws Exception {
        WorkerPool pool = mock(WorkerPool.class);
        AutobatcherEventHandler<String, String> handler = new IndependentBatchingEventHandler<>(s -> {}, 1, pool);
        handler.close();
        verify(pool).close();
    }

    @Test
    public void handlerDoesNotInvokeBatchingFunctionIfEndOfBatchIsNotSignalled() throws Exception {
        try (handler) {
            handler.onEvent(createBatchElement("element1"), IGNORED_SEQUENCE, false);
            handler.onEvent(createBatchElement("element2"), IGNORED_SEQUENCE, false);
        }
        batchFunction.assertNoBatchWasProcessed();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 5})
    public void handlerInvokesBatchingFunctionOnEndOfBatch(int batchCount) throws Exception {
        List<BatchElement<String, String>> batch = createBatch(batchCount);
        try (handler) {
            submitBatch(handler, batch);
        }
        batchFunction.assertOnlyBatchWasProcessed(batch);
    }

    @Test
    public void handlerDoesNotRemoveDuplicateElements() throws Exception {
        List<BatchElement<String, String>> batch =
                List.of(createBatchElement("element"), createBatchElement("element"));
        try (handler) {
            submitBatch(handler, batch);
        }
        batchFunction.assertOnlyBatchWasProcessed(batch);
    }

    @Test
    public void handlerInvokesBatchingFunctionForEachBatchInSequentialOrder() throws Exception {
        List<BatchElement<String, String>> batch1 = createBatch(2);
        List<BatchElement<String, String>> batch2 = createBatch(3);
        List<BatchElement<String, String>> batch3 = createBatch(1);
        List<BatchElement<String, String>> batch4 = createBatch(4);

        try (handler) {
            submitBatch(handler, batch1);
            submitBatch(handler, batch2);
            submitBatch(handler, batch3);
            submitBatch(handler, batch4);
        }

        batchFunction.assertOnlyBatchesWereProcessedInOrder(List.of(batch1, batch2, batch3, batch4));
    }

    private static void submitBatch(
            AutobatcherEventHandler<String, String> handler, List<BatchElement<String, String>> batch) {
        for (int i = 0; i < batch.size(); i++) {
            try {
                handler.onEvent(batch.get(i), IGNORED_SEQUENCE, i == batch.size() - 1);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static List<BatchElement<String, String>> createBatch(int batchSize) {
        String batchId = UUID.randomUUID().toString();
        return IntStream.range(0, batchSize)
                .mapToObj(i -> createBatchElement(batchId + "-element" + i))
                .collect(Collectors.toList());
    }

    private static BatchElement<String, String> createBatchElement(String input) {
        return BatchElement.of(
                input,
                new DisruptorAutobatcher.DisruptorFuture<>(
                        Ticker.systemTicker(),
                        AutobatcherTelemetryComponents.create("test", new DefaultTaggedMetricRegistry())));
    }

    private static final class StubbedBatchFunction implements Consumer<List<BatchElement<String, String>>> {
        private final List<List<BatchElement<String, String>>> processedBatches =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        public void accept(List<BatchElement<String, String>> batch) {
            List<BatchElement<String, String>> copy = ImmutableList.copyOf(batch);
            processedBatches.add(copy);
        }

        void assertOnlyBatchWasProcessed(List<BatchElement<String, String>> batch) {
            synchronized (processedBatches) {
                assertThat(processedBatches).containsOnly(batch);
            }
        }

        void assertOnlyBatchesWereProcessedInOrder(List<List<BatchElement<String, String>>> batches) {
            synchronized (processedBatches) {
                assertThat(processedBatches).containsExactlyElementsOf(batches);
            }
        }

        void assertNoBatchWasProcessed() {
            synchronized (processedBatches) {
                assertThat(processedBatches).isEmpty();
            }
        }
    }
}
