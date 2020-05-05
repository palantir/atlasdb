/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

public class UnstableOrderedIterableTest {
    @Test
    public void orderingIsUnstable() {
        // Strobes once in 1000000! times. We can live with that.
        List<Integer> numbers = IntStream.range(0, 1000000).boxed().collect(Collectors.toList());
        Iterable<Integer> numbersIterable = new UnstableOrderedIterable<>(numbers);

        List<Integer> iterationOrder = Lists.newArrayList();
        numbersIterable.iterator().forEachRemaining(iterationOrder::add);

        List<Integer> secondIterationOrder = Lists.newArrayList();
        numbersIterable.iterator().forEachRemaining(secondIterationOrder::add);

        assertThat(iterationOrder).isNotEqualTo(secondIterationOrder);

        // hasSameElementsAs() etc. seem to be inefficient
        assertThat(ImmutableSet.copyOf(iterationOrder)).isEqualTo(ImmutableSet.copyOf(numbers));
    }
}
