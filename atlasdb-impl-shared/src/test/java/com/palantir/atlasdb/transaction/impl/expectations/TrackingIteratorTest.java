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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.ToLongFunction;
import one.util.streamex.StreamEx;
import org.junit.Test;

public final class TrackingIteratorTest {
    private static final String STRING = "test";
    // this has to be an anonymous inner class rather than a lambda in order to spy
    private static final ToLongFunction<String> STRING_LENGTH_MEASURER = new ToLongFunction<>() {
        @Override
        public long applyAsLong(String value) {
            return value.length();
        }
    };

    @Test
    public void oneElementTrackingIteratorIsWiredCorrectly() {
        BytesReadTracker mockTracker = mock(BytesReadTracker.class);
        ToLongFunction<String> measurer = spy(STRING_LENGTH_MEASURER);

        Iterator<String> oneElementIterator = ImmutableList.of(STRING).stream().iterator();

        TrackingIterator<String, Iterator<String>> trackingIterator =
                new TrackingIterator<>(oneElementIterator, mockTracker, measurer);

        assertThat(trackingIterator).toIterable().containsExactlyElementsOf(ImmutableList.of(STRING));
        verify(measurer).applyAsLong(STRING);
        verify(mockTracker).record(STRING_LENGTH_MEASURER.applyAsLong(STRING));
        verifyNoMoreInteractions(mockTracker, measurer);
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
        ToLongFunction<String> measurer = spy(STRING_LENGTH_MEASURER);
        when(measurer.applyAsLong(anyString())).thenThrow(RuntimeException.class);

        TrackingIterator<String, Iterator<String>> trackingIterator =
                new TrackingIterator<>(createStringIterator(), _unused -> {}, measurer);

        assertThat(trackingIterator)
                .toIterable()
                .containsExactlyElementsOf(ImmutableList.copyOf(createStringIterator()));
    }

    @Test
    public void trackingIteratorForwardsValuesDespiteExceptionAtConsumption() {
        BytesReadTracker mockConsumer = mock(BytesReadTracker.class);
        doThrow(RuntimeException.class).when(mockConsumer).record(anyLong());

        TrackingIterator<String, Iterator<String>> trackingIterator =
                new TrackingIterator<>(createStringIterator(), mockConsumer, STRING_LENGTH_MEASURER);

        assertThat(trackingIterator)
                .toIterable()
                .containsExactlyElementsOf(ImmutableList.copyOf(createStringIterator()));
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
