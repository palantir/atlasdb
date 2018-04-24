/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Test;

public class JavaSuppliersTest {
    @Test
    public void doesNotInvokeFunctionTillGet() {
        Function<String, Integer> throwingFunction = unused -> {
            throw new IllegalArgumentException();
        };
        Supplier<String> stringSupplier = () -> "foo";

        Supplier<Integer> composed = JavaSuppliers.compose(throwingFunction, stringSupplier);

        assertThatThrownBy(composed::get).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void changesInSuppliedValuesPassedThroughFunction() {
        Function<Integer, Integer> timesTwo = num -> num * 2;
        AtomicInteger atomicInteger = new AtomicInteger();
        Supplier<Integer> integerSupplier = atomicInteger::incrementAndGet;

        Supplier<Integer> composed = JavaSuppliers.compose(timesTwo, integerSupplier);
        assertThat(composed.get()).isEqualTo(2);
        assertThat(composed.get()).isEqualTo(4);
        assertThat(composed.get()).isEqualTo(6);
    }
}
