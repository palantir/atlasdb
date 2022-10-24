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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.Test;

public class TrackingClosableIteratorTest {
    private static final ImmutableList<String> STRINGS =
            ImmutableList.of("test1", "test200", "composite", "", "t", "tt");
    private static final Function<String, Long> MEASURER = Functions.compose(Long::valueOf, String::length);

    @Test
    public void trackingStringIteratorForwardsData() {
        ClosableIterator<String> iterator = spawnIterator();
        TrackingClosableIterator<String> trackingIterator =
                new TrackingClosableIterator<>(spawnIterator(), noOp(), MEASURER);

        trackingIterator.forEachRemaining(string -> {
            assertTrue(iterator.hasNext());
            assertEquals(iterator.next(), string);
        });

        assertFalse(iterator.hasNext());
    }

    @Test
    public void trackingStringIteratorTracksData() {
        ClosableIterator<String> iterator = spawnIterator();

        TrackingClosableIterator<String> trackingIterator = new TrackingClosableIterator<>(
                spawnIterator(),
                new Consumer<Long>() {
                    final ClosableIterator<String> baseIterator = spawnIterator();

                    @Override
                    public void accept(Long bytes) {
                        assertEquals(MEASURER.apply(baseIterator.next()), bytes);
                    }
                },
                MEASURER);

        trackingIterator.forEachRemaining(noOp());
    }

    @Test
    public void trackingStringIteratorDelegatesClose() {
        ClosableIterator<String> iterator = spy(spawnIterator());
        TrackingClosableIterator<String> trackingIterator = new TrackingClosableIterator<>(iterator, noOp(), MEASURER);
        trackingIterator.close();
        verify(iterator, times(1)).close();
    }

    private static ClosableIterator<String> spawnIterator() {
        return ClosableIterators.wrapWithEmptyClose(STRINGS.stream().iterator());
    }

    private static <T> Consumer<T> noOp() {
        return t -> {};
    }
}
