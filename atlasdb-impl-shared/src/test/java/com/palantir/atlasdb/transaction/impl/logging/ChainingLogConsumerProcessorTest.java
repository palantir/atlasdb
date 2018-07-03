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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.function.Supplier;

import org.junit.Test;

public class ChainingLogConsumerProcessorTest {
    private static final LogConsumerProcessor DELEGATING_PROCESSOR_1 = Supplier::get;
    private static final LogConsumerProcessor DELEGATING_PROCESSOR_2 = Supplier::get;

    private static final LogConsumerProcessor DUPLICATING_PROCESSOR = templateSupplier ->
            assertThat(templateSupplier.get()).isEqualTo(templateSupplier.get());

    @SuppressWarnings("checkstyle:WhitespaceAround")
    private static final LogConsumerProcessor SWALLOWING_PROCESSOR = unused -> {};

    @SuppressWarnings("unchecked") // Mocking
    private final Supplier<LogTemplate> templateSupplier = mock(Supplier.class);

    private LogConsumerProcessor chainingProcessor;

    @Test
    public void doesNotInvokeSupplierIfNoDelegates() {
        givenProcessors();
        whenChainingProcessorIsCalled();
        thenSupplierIsNotCalled();
    }

    @Test
    public void sendsSupplierToDelegate() {
        givenProcessors(DELEGATING_PROCESSOR_1);
        whenChainingProcessorIsCalled();
        thenSupplierIsCalledOnce();
    }

    @Test
    public void onlyInvokesSupplierOnceEvenIfDelegateProcessorCallsItMultipleTimes() {
        givenProcessors(DUPLICATING_PROCESSOR);
        whenChainingProcessorIsCalled();
        thenSupplierIsCalledOnce();
    }

    @Test
    public void onlyInvokesSupplierOnceEvenIfMultipleDelegateProcessorsCallIt() {
        givenProcessors(DELEGATING_PROCESSOR_1, DELEGATING_PROCESSOR_2);
        whenChainingProcessorIsCalled();
        thenSupplierIsCalledOnce();
    }

    @Test
    public void onlyInvokesSupplierIfSomeDelegateProcessorCallsIt() {
        givenProcessors(SWALLOWING_PROCESSOR);
        whenChainingProcessorIsCalled();
        thenSupplierIsNotCalled();
    }

    private void givenProcessors(LogConsumerProcessor... processors) {
        chainingProcessor = ImmutableChainingLogConsumerProcessor.builder()
                .addProcessors(processors)
                .build();
    }

    private void whenChainingProcessorIsCalled() {
        chainingProcessor.maybeLog(templateSupplier);
    }

    private void thenSupplierIsNotCalled() {
        verify(templateSupplier, never()).get();
    }

    private void thenSupplierIsCalledOnce() {
        verify(templateSupplier, times(1)).get();
    }
}
