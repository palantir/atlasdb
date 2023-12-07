/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.lmax.disruptor.EventHandler;
import java.util.function.Supplier;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

public final class MultiplexingEventHandlerTest {

    @Test
    public void test() throws Exception {
        EventHandler<BatchElement<String, String>> handler1 = mock(EventHandler.class);
        EventHandler<BatchElement<String, String>> handler2 = mock(EventHandler.class);
        InOrder inOrder = inOrder(handler1, handler2);

        Supplier<EventHandler<BatchElement<String, String>>> supplier = mock(Supplier.class);
        when(supplier.get()).thenReturn(handler1).thenReturn(handler2);

        DeterministicScheduler executorService = new DeterministicScheduler();
        EventHandler<BatchElement<String, String>> handler =
                new MultiplexingEventHandler<>(2, executorService, supplier);

        BatchElement<String, String> element1 = createBatchElement("1");
        BatchElement<String, String> element2 = createBatchElement("2");
        BatchElement<String, String> element3 = createBatchElement("3");
        BatchElement<String, String> element4 = createBatchElement("4");
        BatchElement<String, String> element5 = createBatchElement("5");
        BatchElement<String, String> element6 = createBatchElement("6");

        handler.onEvent(element1, 10, false);
        handler.onEvent(element2, 10, false);
        handler.onEvent(element3, 10, false);
        handler.onEvent(element4, 10, true);
        handler.onEvent(element5, 10, true);
        handler.onEvent(element6, 10, true);

        executorService.runUntilIdle();

        inOrder.verify(handler1).onEvent(element1, 10, false);
        inOrder.verify(handler1).onEvent(element2, 10, false);
        inOrder.verify(handler1).onEvent(element3, 10, false);
        inOrder.verify(handler1).onEvent(element4, 10, true);
        inOrder.verify(handler2).onEvent(element5, 10, true);
        inOrder.verify(handler1).onEvent(element6, 10, true);
        inOrder.verifyNoMoreInteractions();
    }

    @SuppressWarnings("DoNotMock")
    private BatchElement<String, String> createBatchElement(String input) {
        return BatchElement.of(input, mock(DisruptorAutobatcher.DisruptorFuture.class));
    }
}
