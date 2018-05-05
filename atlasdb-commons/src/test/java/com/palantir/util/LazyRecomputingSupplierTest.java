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

import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;

public class LazyRecomputingSupplierTest {
    private int counter;
    private int delegateValue;
    private Supplier<Integer> delegateSupplier = () -> delegateValue;
    private Supplier<Integer> supplier = new LazyRecomputingSupplier<>(delegateSupplier, this::incrementCounter);


    @Before
    public void setup() {
        setDelegateToReturn(1);
        counter = 1;
    }

    @Test
    public void firstGetCallsRecomputingFunction() {
        assertThat(supplier.get()).isEqualTo(2);
    }

    @Test
    public void firstGetReflectsChangesToDelegate() {
        setDelegateToReturn(10);
        assertThat(supplier.get()).isEqualTo(11);
    }

    @Test
    public void multipleGetsWithSameSupplierDoNotRecompute() {
        assertThat(supplier.get()).isEqualTo(2);
        assertThat(supplier.get()).isEqualTo(2);
        assertThat(supplier.get()).isEqualTo(2);
    }

    @Test
    public void getRecomputesWhenDelegateValueChanges() {
        assertThat(supplier.get()).isEqualTo(2);

        setDelegateToReturn(10);
        assertThat(supplier.get()).isEqualTo(12);

        setDelegateToReturn(1);
        assertThat(supplier.get()).isEqualTo(13);
    }

    @Test
    public void getRecomputesOnlyForLatestChange() {
        assertThat(supplier.get()).isEqualTo(2);

        setDelegateToReturn(10);
        setDelegateToReturn(20);
        setDelegateToReturn(2);
        assertThat(supplier.get()).isEqualTo(4);
        assertThat(supplier.get()).isEqualTo(4);
    }

    @Test
    public void getDoesNotRecomputeIfLatestChangeEffectivelyNoOp() {
        assertThat(supplier.get()).isEqualTo(2);

        setDelegateToReturn(10);
        setDelegateToReturn(20);
        setDelegateToReturn(1);
        assertThat(supplier.get()).isEqualTo(2);
        assertThat(supplier.get()).isEqualTo(2);
    }

    private int incrementCounter(int increment) {
        counter += increment;
        return counter;
    }

    private void setDelegateToReturn(int value) {
        delegateValue = value;
    }
}
