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

import com.google.common.collect.ImmutableList;
import java.util.Comparator;
import org.junit.Test;

public class UnstableOrderedIterableTest {
    @Test
    public void orderingIsUnstable() {
        Iterable<Integer> numbers = UnstableOrderedIterable.create(ImmutableList.of(1, 2), Comparator.naturalOrder());
        assertThat(ImmutableList.copyOf(numbers)).containsExactly(1, 2);
        assertThat(ImmutableList.copyOf(numbers)).containsExactly(2, 1);
        assertThat(ImmutableList.copyOf(numbers)).containsExactly(1, 2);
    }
}
