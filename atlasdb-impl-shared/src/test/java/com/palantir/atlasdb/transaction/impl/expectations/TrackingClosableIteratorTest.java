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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.collect.ImmutableList;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;
import org.junit.Test;

public final class TrackingClosableIteratorTest {
    @Test
    public void trackingClosableStringIteratorDelegatesClose() {
        ClosableIterator<String> iterator = spy(createClosableStringIterator());
        TrackingClosableIterator<String> trackingIterator = new TrackingClosableIterator<>(
                iterator, TrackingIteratorTestUtils.noOp(), TrackingIteratorTestUtils.STRING_MEASURER);
        trackingIterator.close();
        verify(iterator, times(1)).close();
        verifyNoMoreInteractions(iterator);
    }

    @Test
    public void trackingClosableStringIteratorIsWiredCorrectly() {
        Consumer<Long> tracker = spy(TrackingIteratorTestUtils.noOp());
        ToLongFunction<String> measurer = spy(TrackingIteratorTestUtils.STRING_MEASURER);
        TrackingClosableIterator<String> trackingIterator =
                new TrackingClosableIterator<>(createClosableStringIterator(), tracker, measurer);

        assertThat(trackingIterator)
                .toIterable()
                .containsExactlyElementsOf(ImmutableList.copyOf(createClosableStringIterator()));

        try (ClosableIterator<String> iterator = createClosableStringIterator()) {
            iterator.forEachRemaining(string -> {
                verify(measurer, times(1)).applyAsLong(string);
                verify(tracker, times(1)).accept(TrackingIteratorTestUtils.STRING_MEASURER.applyAsLong(string));
            });
        }

        verifyNoMoreInteractions(measurer);
        verifyNoMoreInteractions(tracker);
    }

    private static ClosableIterator<String> createClosableStringIterator() {
        return ClosableIterators.wrapWithEmptyClose(TrackingIteratorTestUtils.createStringIterator());
    }
}
