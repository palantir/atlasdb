/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.transaction.impl.logging;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;

public class PredicateBackedLogConsumerProcessorTest {
    private static final LogTemplate LOG_TEMPLATE = ImmutableLogTemplate.of("{}", new String[1]);

    @SuppressWarnings("unchecked") // Mocking
    private final BiConsumer<String, Object[]> biConsumer = mock(BiConsumer.class);
    @SuppressWarnings("unchecked") // Mocking
    private final Supplier<LogTemplate> logTemplateSupplier = mock(Supplier.class);

    @Before
    public void setup() {
        when(logTemplateSupplier.get()).thenReturn(LOG_TEMPLATE);
    }

    @Test
    public void doesNotInvokeLogSupplierIfBooleanSupplierReturnsFalse() {
        LogConsumerProcessor processor = PredicateBackedLogConsumerProcessor.create(biConsumer, () -> false);
        processor.maybeLog(logTemplateSupplier);
        verify(logTemplateSupplier, never()).get();
        verify(biConsumer, never()).accept(any(), any());
    }

    @Test
    public void invokesLogSupplierIfBooleanSupplierReturnsTrue() {
        LogConsumerProcessor processor = PredicateBackedLogConsumerProcessor.create(biConsumer, () -> true);
        processor.maybeLog(logTemplateSupplier);
        verify(logTemplateSupplier, times(1)).get();
        verify(biConsumer, times(1)).accept(LOG_TEMPLATE.format(), LOG_TEMPLATE.arguments());
    }

    @Test
    public void responsiveToChangesInBooleansReturned() {
        AtomicInteger atomicInteger = new AtomicInteger();
        LogConsumerProcessor processor =
                PredicateBackedLogConsumerProcessor.create(biConsumer, () -> atomicInteger.incrementAndGet() % 2 == 0);

        // integer is 1, so false
        processor.maybeLog(logTemplateSupplier);
        verify(logTemplateSupplier, never()).get();
        verify(biConsumer, never()).accept(any(), any());

        // integer is 2, so true
        processor.maybeLog(logTemplateSupplier);
        verify(logTemplateSupplier, times(1)).get();
        verify(biConsumer, times(1)).accept(LOG_TEMPLATE.format(), LOG_TEMPLATE.arguments());
    }
}
