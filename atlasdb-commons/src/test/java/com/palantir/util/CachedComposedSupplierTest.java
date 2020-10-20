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
package com.palantir.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.palantir.common.concurrent.PTExecutors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class CachedComposedSupplierTest {
    private long counter;
    private long supplierCounter;
    private Supplier<VersionedType<Long>> mockVersionedSupplier;
    private Supplier<Long> testSupplier;

    @Before
    public void setup() {
        counter = 0;
        supplierCounter = 0;
        mockVersionedSupplier = Mockito.mock(Supplier.class);
        testSupplier = new CachedComposedSupplier<>(this::countingFunction, mockVersionedSupplier);
    }

    @Test
    public void appliesFunctionToNullValue() {
        when(mockVersionedSupplier.get()).thenReturn(VersionedType.of(null, 0));
        assertThat(counter).isEqualTo(0);

        assertThat(testSupplier.get()).isNull();
        assertThat(counter).isEqualTo(1);
    }

    @Test
    public void appliesFunctionOnlyOnceWhenUnderlyingSupplierIsConstant() {
        when(mockVersionedSupplier.get()).thenReturn(VersionedType.of(3L, 0));
        assertThat(counter).isEqualTo(0);

        assertThat(testSupplier.get()).isEqualTo(6);
        assertThat(testSupplier.get()).isEqualTo(6);
        assertThat(testSupplier.get()).isEqualTo(6);
        assertThat(counter).isEqualTo(1);
    }

    @Test
    public void appliesFunctionEachTimeGetIsInvokedAndSuppliedVersionChanged() {
        when(mockVersionedSupplier.get()).thenReturn(VersionedType.of(null, 0));
        assertThat(testSupplier.get()).isNull();
        assertThat(counter).isEqualTo(1);

        when(mockVersionedSupplier.get()).thenReturn(VersionedType.of(3L, 1));
        assertThat(testSupplier.get()).isEqualTo(6);
        assertThat(counter).isEqualTo(2);

        when(mockVersionedSupplier.get()).thenReturn(VersionedType.of(3L, 2));
        assertThat(testSupplier.get()).isEqualTo(6);
        assertThat(testSupplier.get()).isEqualTo(6);
        assertThat(counter).isEqualTo(3);

        when(mockVersionedSupplier.get()).thenReturn(VersionedType.of(8L, 3));
        assertThat(testSupplier.get()).isEqualTo(16);
        assertThat(counter).isEqualTo(4);
    }

    @Test
    public void doesNotApplyFunctionIfGetIsInvokedAndSuppliedVersionConstant() {
        when(mockVersionedSupplier.get()).thenReturn(VersionedType.of(null, 0));
        assertThat(testSupplier.get()).isNull();
        assertThat(counter).isEqualTo(1);

        when(mockVersionedSupplier.get()).thenReturn(VersionedType.of(3L, 0));
        assertThat(testSupplier.get()).isNull();
        assertThat(counter).isEqualTo(1);

        when(mockVersionedSupplier.get()).thenReturn(VersionedType.of(8L, 0));
        assertThat(testSupplier.get()).isNull();
        assertThat(counter).isEqualTo(1);

        when(mockVersionedSupplier.get()).thenReturn(VersionedType.of(3L, 1));
        assertThat(testSupplier.get()).isEqualTo(6);
        assertThat(counter).isEqualTo(2);
    }

    @Test
    public void appliesFunctionExactlyOncePerSuppliedValueChange() throws InterruptedException, ExecutionException {
        testSupplier = new CachedComposedSupplier<>(this::countingFunction, this::increasingNumber);
        ExecutorService executorService = PTExecutors.newFixedThreadPool(16);
        for (int i = 0; i < 100_000; i++) {
            executorService.submit(testSupplier::get);
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);

        assertThat(supplierCounter).isGreaterThan(100_000);
        assertThat(counter).isEqualTo(1 + supplierCounter / 100);
    }

    @Test
    public void recomputesIfSupplierHasNotUpdatedForTooLong() throws InterruptedException {
        AtomicLong clockCounter = new AtomicLong();
        testSupplier = new CachedComposedSupplier<>(this::countingFunction, this::constantNumber, 5, clockCounter::get);
        for (int i = 0; i < 25; i++) {
            clockCounter.incrementAndGet();
            testSupplier.get();
        }
        assertThat(counter).isEqualTo(5);
    }

    private Long countingFunction(Long input) {
        counter++;
        if (input == null) {
            return null;
        }
        return input * 2;
    }

    private synchronized VersionedType<Long> increasingNumber() {
        supplierCounter++;
        return VersionedType.of(supplierCounter, supplierCounter / 100);
    }

    private VersionedType<Long> constantNumber() {
        return VersionedType.of(1L, 0);
    }
}
