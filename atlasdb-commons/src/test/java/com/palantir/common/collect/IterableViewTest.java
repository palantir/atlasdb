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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import java.util.List;
import java.util.Spliterator;
import java.util.stream.StreamSupport;
import org.junit.Test;

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
        ImmutableList<String> delegate = ImmutableList.of("a", "b");
        Spliterator<String> spliterator = IterableView.of(delegate).spliterator();
        assertThat(spliterator.estimateSize()).isEqualTo(2);
        assertThat(spliterator.characteristics()).isNotZero();
        assertThat(spliterator.hasCharacteristics(Spliterator.SIZED)).isTrue();
        assertThat(spliterator.hasCharacteristics(Spliterator.SUBSIZED)).isTrue();
        assertThat(StreamSupport.stream(spliterator, false)).containsExactly("a", "b");
    }

    @Test
    public void listPartitionSpliterator() {
        ImmutableList<String> delegate = ImmutableList.of("a", "b", "c");
        IterableView<List<String>> view = IterableView.of(delegate).partition(1);
        Spliterator<List<String>> spliterator = view.spliterator();
        assertThat(spliterator.estimateSize()).isEqualTo(3);
        assertThat(spliterator.characteristics()).isNotZero();
        assertThat(spliterator.hasCharacteristics(Spliterator.SIZED)).isTrue();
        assertThat(spliterator.hasCharacteristics(Spliterator.SUBSIZED)).isTrue();
        assertThat(StreamSupport.stream(spliterator, false)).containsExactly(
                ImmutableList.of("a"),
                ImmutableList.of("b"),
                ImmutableList.of("c"));
    }

    @Test
    public void listTransformSpliterator() {
        ImmutableList<String> delegate = ImmutableList.of("a", "b", "c");
        IterableView<String> view = IterableView.of(delegate).transform(String::toUpperCase);
        Spliterator<String> spliterator = view.spliterator();
        assertThat(spliterator.estimateSize()).isEqualTo(3);
        assertThat(spliterator.characteristics()).isNotZero();
        assertThat(spliterator.hasCharacteristics(Spliterator.SIZED)).isTrue();
        assertThat(spliterator.hasCharacteristics(Spliterator.SUBSIZED)).isTrue();
        assertThat(StreamSupport.stream(spliterator, false)).containsExactly("A", "B", "C");
    }

    @Test
    public void setSpliterator() {
        Spliterator<String> spliterator = IterableView.of(ImmutableSortedSet.of("a", "b")).spliterator();
        assertThat(spliterator.estimateSize()).isEqualTo(2);
        assertThat(spliterator.characteristics()).isNotZero();
        assertThat(spliterator.hasCharacteristics(Spliterator.DISTINCT)).isTrue();
        assertThat(spliterator.hasCharacteristics(Spliterator.SIZED)).isTrue();
        assertThat(spliterator.hasCharacteristics(Spliterator.SUBSIZED)).isTrue();
        assertThat(StreamSupport.stream(spliterator, false)).containsExactly("a", "b");
    }

    @Test
    public void iterablesSpliterator() {
        IterableView<List<String>> view = IterableView.of(Iterables.partition(ImmutableSet.of("a", "b", "c"), 1));
        Spliterator<List<String>> spliterator = view.spliterator();
        assertThat(spliterator.estimateSize()).isEqualTo(Long.MAX_VALUE);
        assertThat(spliterator.characteristics()).isZero();
        assertThat(StreamSupport.stream(spliterator, false)).containsExactly(
                ImmutableList.of("a"),
                ImmutableList.of("b"),
                ImmutableList.of("c"));
    }
}
