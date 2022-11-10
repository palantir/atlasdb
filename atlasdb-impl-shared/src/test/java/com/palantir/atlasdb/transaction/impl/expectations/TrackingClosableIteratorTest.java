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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;
import one.util.streamex.StreamEx;
import org.junit.Test;

public final class TrackingClosableIteratorTest {
    private static final String STRING = "test";
    private static final ClosableIterator<String> ONE_ELEMENT_ITERATOR =
            ClosableIterators.wrapWithEmptyClose(List.of(STRING).iterator());
    private static final ImmutableList<String> STRINGS = ImmutableList.of(
            "test4", "test4", "test200", "composite", "", "t", "twentyElementString1", "tt", "twentyElementString2");

    // these have to be anonymous inner classes rather than lambdas in order to spy
    private static final Consumer<Long> NO_OP = new Consumer<>() {
        @Override
        public void accept(Long _unused) {}
    };
    private static final ToLongFunction<String> STRING_LENGTH_MEASURER = new ToLongFunction<>() {
        @Override
        public long applyAsLong(String value) {
            return value.length();
        }
    };

    @Test
    public void oneElementTrackingClosableIteratorIsWiredCorrectly() {
        Consumer<Long> tracker = spy(NO_OP);
        ToLongFunction<String> measurer = spy(STRING_LENGTH_MEASURER);
        TrackingClosableIterator<String> trackingIterator =
                new TrackingClosableIterator<>(ONE_ELEMENT_ITERATOR, tracker, measurer);

        assertThat(trackingIterator).toIterable().containsExactlyElementsOf(List.of(STRING));
        verify(measurer).applyAsLong(STRING);
        verify(tracker).accept(STRING_LENGTH_MEASURER.applyAsLong(STRING));
        verifyNoMoreInteractions(tracker, measurer);
    }

    @Test
    public void multiElementTrackingClosableIteratorIsWiredCorrectly() {
        ArrayList<Long> consumed = new ArrayList<>();
        TrackingClosableIterator<String> trackingIterator =
                new TrackingClosableIterator<>(createClosableStringIterator(), consumed::add, STRING_LENGTH_MEASURER);

        assertThat(trackingIterator)
                .toIterable()
                .containsExactlyElementsOf(ImmutableList.copyOf(createClosableStringIterator()));

        assertThat(consumed)
                .containsExactlyElementsOf(StreamEx.of(createClosableStringIterator())
                        .mapToLong(STRING_LENGTH_MEASURER)
                        .boxed()
                        .toList());
    }

    @Test
    public void trackingClosableStringIteratorDelegatesClose() {
        ClosableIterator<String> iterator = spy(createClosableStringIterator());
        TrackingClosableIterator<String> trackingIterator =
                new TrackingClosableIterator<>(iterator, NO_OP, STRING_LENGTH_MEASURER);
        trackingIterator.close();
        verify(iterator).close();
        verifyNoMoreInteractions(iterator);
    }

    @Test
    public void trackingIteratorForwardsValuesDespiteExceptionAtMeasurement() {
        ToLongFunction<String> measurer = spy(STRING_LENGTH_MEASURER);
        when(measurer.applyAsLong(anyString())).thenThrow(RuntimeException.class);

        ClosableIterator<String> trackingIterator =
                new TrackingClosableIterator<>(createClosableStringIterator(), NO_OP, measurer);

        assertThat(trackingIterator)
                .toIterable()
                .containsExactlyElementsOf(ImmutableList.copyOf(createClosableStringIterator()));
    }

    @Test
    public void trackingIteratorForwardsValuesDespiteExceptionAtConsumption() {
        Consumer<Long> consumer = spy(NO_OP);
        doThrow(RuntimeException.class).when(consumer).accept(anyLong());

        ClosableIterator<String> trackingIterator =
                new TrackingClosableIterator<>(createClosableStringIterator(), consumer, STRING_LENGTH_MEASURER);

        assertThat(trackingIterator)
                .toIterable()
                .containsExactlyElementsOf(ImmutableList.copyOf(createClosableStringIterator()));
    }

    private static ClosableIterator<String> createClosableStringIterator() {
        return ClosableIterators.wrapWithEmptyClose(createStringIterator());
    }

    private static Iterator<String> createStringIterator() {
        return STRINGS.stream().iterator();
    }
}
