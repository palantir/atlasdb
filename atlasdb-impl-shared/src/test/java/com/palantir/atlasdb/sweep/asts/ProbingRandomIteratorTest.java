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

package com.palantir.atlasdb.sweep.asts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public final class ProbingRandomIteratorTest {
    private static final List<Integer> LARGE_INT_LIST =
            IntStream.range(0, 1000).boxed().collect(Collectors.toList());

    @Test
    public void iteratorReturnsAllElements() {
        ProbingRandomIterator<Integer> iterator = new ProbingRandomIterator<>(LARGE_INT_LIST);
        List<Integer> elements = ImmutableList.copyOf(iterator);

        // containsExactlyInAnyOrderElementsOf is actually quite slow with large lists (>10000 elements)
        // From a quick glance, it seems to use a O(n^2) algorithm
        assertThat(elements).containsExactlyInAnyOrderElementsOf(LARGE_INT_LIST);
    }

    @Test
    public void iteratorReturnsElementsInDifferentOrderEachTime() {
        List<Integer> elements1 = ImmutableList.copyOf(new ProbingRandomIterator<>(LARGE_INT_LIST));
        List<Integer> elements2 = ImmutableList.copyOf(new ProbingRandomIterator<>(LARGE_INT_LIST));
        assertThat(elements1).isNotEqualTo(elements2);
    }

    @Test
    public void iteratorDoesNotModifyUnderlyingList() {
        List<Integer> originalList = ImmutableList.copyOf(LARGE_INT_LIST);
        List<Integer> _unused = ImmutableList.copyOf(new ProbingRandomIterator<>(LARGE_INT_LIST));
        assertThat(originalList).containsExactlyElementsOf(LARGE_INT_LIST);
    }

    @Test
    public void iteratorDoesNotFailOnEmptyList() {
        List<Integer> emptyList = ImmutableList.of();
        ProbingRandomIterator<Integer> iterator = new ProbingRandomIterator<>(emptyList);
        assertThat(iterator.hasNext()).isFalse();
        assertThat(ImmutableList.copyOf(iterator)).isEmpty();
    }

    @Test
    public void iteratorNextThrowsNoSuchElementExceptionWhenNoMoreElements() {
        ProbingRandomIterator<Integer> iterator = new ProbingRandomIterator<>(ImmutableList.of(1));
        iterator.next();
        assertThat(iterator.hasNext()).isFalse();
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }
}
