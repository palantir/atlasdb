/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.expectations;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.Test;

public class TrackingIteratorTest {
    private static final ImmutableList<String> STRINGS =
            ImmutableList.of("test1", "test200", "composite", "", "t", "tt");
    private static final Function<String, Long> MEASURER = Functions.compose(Long::valueOf, String::length);

    @Test
    public void trackingStringIteratorForwardsData() {
        Iterator<String> iterator = spawnIterator();
        TrackingIterator<String, Iterator<String>> trackingIterator =
                new TrackingIterator<>(spawnIterator(), noOp(), MEASURER);

        trackingIterator.forEachRemaining(string -> {
            assertThat(iterator.hasNext()).isTrue();
            assertThat(string).isEqualTo(iterator.next());
        });

        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void trackingStringIteratorTracksData() {
        Iterator<String> iterator = spawnIterator();

        TrackingIterator<String, Iterator<String>> trackingIterator = new TrackingIterator<>(
                spawnIterator(),
                new Consumer<Long>() {
                    final Iterator<String> baseIterator = spawnIterator();

                    @Override
                    public void accept(Long bytes) {
                        assertThat(bytes).isEqualTo(MEASURER.apply(baseIterator.next()));
                    }
                },
                MEASURER);

        trackingIterator.forEachRemaining(noOp());
    }

    private static Iterator<String> spawnIterator() {
        return STRINGS.stream().iterator();
    }

    private static <T> Consumer<T> noOp() {
        return t -> {};
    }
}
