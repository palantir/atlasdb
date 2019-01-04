/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.common.collect;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Spliterator;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;

public class IterableViewTest {

    @Test
    public void stream() {
        assertThat(IterableView.of(ImmutableList.of("a", "b")).stream()).containsExactly("a", "b");
        assertThat(IterableView.of(
                Iterables.limit(Iterables.cycle(ImmutableSet.of("a", "b", "c")), 4)).stream())
                .containsExactly("a", "b", "c", "a");
        assertThat(IterableView.of(ImmutableList.of("a", "b")).transform(String::toUpperCase)
                .stream())
                .containsExactly("A", "B");
    }

    @Test
    public void listSpliterator() {
        Spliterator<String> spliterator = IterableView.of(ImmutableList.of("a", "b")).spliterator();
        assertThat(spliterator.estimateSize()).isEqualTo(2);
        int characteristics = spliterator.characteristics();
        assertThat(characteristics).isNotEqualTo(0);
        assertThat(characteristics & Spliterator.CONCURRENT).isEqualTo(0);
        assertThat(characteristics & Spliterator.DISTINCT).isEqualTo(0);
        assertThat(characteristics & Spliterator.IMMUTABLE).isEqualTo(Spliterator.IMMUTABLE);
        assertThat(characteristics & Spliterator.NONNULL).isEqualTo(Spliterator.NONNULL);
        assertThat(characteristics & Spliterator.SIZED).isEqualTo(Spliterator.SIZED);
        assertThat(characteristics & Spliterator.SORTED).isEqualTo(0);
        assertThat(characteristics & Spliterator.SUBSIZED).isEqualTo(Spliterator.SUBSIZED);
    }

    @Test
    public void listPartitionSpliterator() {
        IterableView<List<String>> view = IterableView.of(ImmutableList.of("a", "b", "c")).partition(1);
        Spliterator<List<String>> spliterator = view.spliterator();
        assertThat(spliterator.estimateSize()).isEqualTo(3);
        int characteristics = spliterator.characteristics();
        assertThat(characteristics).isNotEqualTo(0);
        assertThat(characteristics & Spliterator.CONCURRENT).isEqualTo(0);
        assertThat(characteristics & Spliterator.DISTINCT).isEqualTo(0);
        assertThat(characteristics & Spliterator.IMMUTABLE).isEqualTo(0);
        assertThat(characteristics & Spliterator.NONNULL).isEqualTo(0);
        assertThat(characteristics & Spliterator.SIZED).isEqualTo(Spliterator.SIZED);
        assertThat(characteristics & Spliterator.SORTED).isEqualTo(0);
        assertThat(characteristics & Spliterator.SUBSIZED).isEqualTo(Spliterator.SUBSIZED);
    }

    @Test
    public void listTransformSpliterator() {
        IterableView<String> view = IterableView.of(ImmutableList.of("a", "b", "c")).transform(String::toUpperCase);
        Spliterator<String> spliterator = view.spliterator();
        assertThat(spliterator.estimateSize()).isEqualTo(3);
        int characteristics = spliterator.characteristics();
        assertThat(characteristics).isNotEqualTo(0);
        assertThat(characteristics & Spliterator.CONCURRENT).isEqualTo(0);
        assertThat(characteristics & Spliterator.DISTINCT).isEqualTo(0);
        assertThat(characteristics & Spliterator.IMMUTABLE).isEqualTo(0);
        assertThat(characteristics & Spliterator.NONNULL).isEqualTo(0);
        assertThat(characteristics & Spliterator.SIZED).isEqualTo(Spliterator.SIZED);
        assertThat(characteristics & Spliterator.SORTED).isEqualTo(0);
        assertThat(characteristics & Spliterator.SUBSIZED).isEqualTo(Spliterator.SUBSIZED);
    }

    @Test
    public void setSpliterator() {
        Spliterator<String> spliterator = IterableView.of(ImmutableSortedSet.of("a", "b")).spliterator();
        assertThat(spliterator.estimateSize()).isEqualTo(2);
        int characteristics = spliterator.characteristics();
        assertThat(characteristics).isNotEqualTo(0);
        assertThat(characteristics & Spliterator.CONCURRENT).isEqualTo(0);
        assertThat(characteristics & Spliterator.DISTINCT).isEqualTo(Spliterator.DISTINCT);
        assertThat(characteristics & Spliterator.IMMUTABLE).isEqualTo(Spliterator.IMMUTABLE);
        assertThat(characteristics & Spliterator.NONNULL).isEqualTo(Spliterator.NONNULL);
        assertThat(characteristics & Spliterator.SIZED).isEqualTo(Spliterator.SIZED);
        assertThat(characteristics & Spliterator.SORTED).isEqualTo(Spliterator.SORTED);
        assertThat(characteristics & Spliterator.SUBSIZED).isEqualTo(Spliterator.SUBSIZED);
    }

    @Test
    public void iterablesSpliterator() {
        IterableView<List<String>> view = IterableView.of(Iterables.partition(ImmutableSet.of("a", "b", "c"), 1));
        Spliterator<List<String>> spliterator = view.spliterator();
        assertThat(spliterator.estimateSize()).isEqualTo(Long.MAX_VALUE);
        int characteristics = spliterator.characteristics();
        assertThat(characteristics).isEqualTo(0);
    }
}
