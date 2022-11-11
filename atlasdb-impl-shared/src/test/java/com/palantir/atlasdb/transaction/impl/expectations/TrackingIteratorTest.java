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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.ToLongFunction;
import one.util.streamex.StreamEx;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class TrackingIteratorTest {
    private static final String STRING = "test";
    // this has to be an anonymous inner class rather than a lambda in order to spy
    private static final ToLongFunction<String> STRING_LENGTH_MEASURER = new ToLongFunction<>() {
        @Override
        public long applyAsLong(String value) {
            return value.length();
        }
    };

    @Mock
    private BytesReadTracker tracker;

    @Mock
    private ToLongFunction<String> measurer;

    @Test
    public void oneElementTrackingIteratorIsWiredCorrectly() {
        long measuredValue = 1L;
        when(measurer.applyAsLong(anyString())).thenReturn(measuredValue);
        when(iterator.next()).thenReturn(STRING).thenThrow(NoSuchElementException.class);

        assertThat(trackingIterator).toIterable().containsExactlyElementsOf(ImmutableList.of(STRING));
        verify(measurer).applyAsLong(STRING);
        verify(tracker).record(STRING_LENGTH_MEASURER.applyAsLong(STRING));
        verifyNoMoreInteractions(tracker, measurer);
    }

    @Test
    public void multiElementTrackingIteratorIsWiredCorrectly() {
        ArrayList<Long> consumed = new ArrayList<>();

        TrackingIterator<String, Iterator<String>> trackingIterator =
                new TrackingIterator<>(createStringIterator(), consumed::add, STRING_LENGTH_MEASURER);

        assertThat(trackingIterator)
                .toIterable()
                .containsExactlyElementsOf(ImmutableList.copyOf(createStringIterator()));

        assertThat(consumed)
                .containsExactlyElementsOf(StreamEx.of(createStringIterator())
                        .mapToLong(STRING_LENGTH_MEASURER)
                        .boxed()
                        .toList());
    }

    @Test
    public void trackingIteratorForwardsValuesDespiteExceptionAtMeasurement() {
        when(measurer.applyAsLong(anyString())).thenThrow(RuntimeException.class);
        Iterator<String> trackingIterator = createTrackingIterator(Iterators.singletonIterator(STRING));
        assertThat(trackingIterator).toIterable().containsExactlyInAnyOrder(STRING);
    }

    @Test
    public void trackingIteratorForwardsValuesDespiteExceptionAtConsumption() {
        doThrow(RuntimeException.class).when(tracker).record(anyLong());
        Iterator<String> trackingIterator = createTrackingIterator(Iterators.singletonIterator(STRING));
        assertThat(trackingIterator).toIterable().containsExactlyInAnyOrder(STRING);
    }

    public Iterator<String> createTrackingIterator(Iterator<String> delegate) {
        return new TrackingIterator<>(delegate, tracker, measurer);
    }

    public static Iterator<String> createStringIterator() {
        ImmutableList<String> STRINGS = ImmutableList.of(
                "test4",
                "test4",
                "test200",
                "composite",
                "",
                "t",
                "twentyElementString1",
                "tt",
                "twentyElementString2");

        return STRINGS.stream().iterator();
    }
}
