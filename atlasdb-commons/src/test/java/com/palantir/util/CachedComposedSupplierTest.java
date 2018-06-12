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

package com.palantir.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.palantir.common.concurrent.PTExecutors;

public class CachedComposedSupplierTest {
    int counter;
    int supplierCounter;

    @Before
    public void setup() {
        counter = 0;
        supplierCounter = 0;
    }

    @Test
    public void appliesFunctionToNull() {
        Supplier<Integer> testSupplier = new CachedComposedSupplier<>(this::countingFunction, () -> null);
        assertThat(counter).isEqualTo(0);

        assertThat(testSupplier.get()).isNull();
        assertThat(counter).isEqualTo(1);
    }

    @Test
    public void appliesFunctionOnlyOnceWhenUnderlyingSupplierIsConstant() {
        Supplier<Integer> testSupplier = new CachedComposedSupplier<>(this::countingFunction, () -> 3);
        assertThat(counter).isEqualTo(0);

        assertThat(testSupplier.get()).isEqualTo(6);
        assertThat(testSupplier.get()).isEqualTo(6);
        assertThat(testSupplier.get()).isEqualTo(6);
        assertThat(counter).isEqualTo(1);
    }


    @Test
    public void appliesFunctionEachTimeGetIsInvokedAndSuppliedValueChanged() {
        Supplier<Integer> mockSupplier = Mockito.mock(Supplier.class);
        Supplier<Integer> testSupplier = new CachedComposedSupplier<>(this::countingFunction, mockSupplier);

        when(mockSupplier.get()).thenReturn(null);
        assertThat(testSupplier.get()).isNull();
        assertThat(counter).isEqualTo(1);

        when(mockSupplier.get()).thenReturn(3);
        assertThat(testSupplier.get()).isEqualTo(6);
        assertThat(counter).isEqualTo(2);

        when(mockSupplier.get()).thenReturn(8);
        assertThat(testSupplier.get()).isEqualTo(16);
        assertThat(testSupplier.get()).isEqualTo(16);
        assertThat(counter).isEqualTo(3);

        when(mockSupplier.get()).thenReturn(3);
        assertThat(testSupplier.get()).isEqualTo(6);
        assertThat(counter).isEqualTo(4);
    }

    @Test
    public void appliesFunctionExactlyOncePerSuppliedValueChange() throws InterruptedException, ExecutionException {
        Supplier<Integer> testSupplier = new CachedComposedSupplier<>(this::countingFunction, this::increasingNumber);
        ExecutorService executorService = PTExecutors.newFixedThreadPool(16);
        for (int i = 0; i < 100_000; i++) {
            executorService.submit(testSupplier::get);
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);

        assertThat(supplierCounter).isGreaterThan(100_000);
        assertThat(counter).isEqualTo(1 + supplierCounter / 100);
    }

    private Integer countingFunction(Integer input) {
        counter++;
        if (input == null) {
            return null;
        }
        return input * 2;
    }

    private synchronized Integer increasingNumber() {
        supplierCounter++;
        return supplierCounter / 100;
    }
}
