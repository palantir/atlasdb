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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.Iterators;
import java.util.Iterator;
import java.util.function.ToLongFunction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class TrackingIteratorTest {
    private static final String STRING_1 = "one";
    private static final String STRING_2 = "two";
    private static final String STRING_3 = "three";

    @Mock
    private BytesReadTracker tracker;

    @Mock
    private ToLongFunction<String> measurer;

    @Test
    public void trackingIteratorDelegatesNext() {
        Iterator<String> trackingIterator = createTrackingIterator(Iterators.forArray(STRING_1, STRING_2, STRING_3));
        assertThat(trackingIterator).toIterable().containsExactly(STRING_1, STRING_2, STRING_3);
    }

    @Test
    public void trackingIteratorTracksAndMeasuresInDelegateIteratorOrder() {
        when(measurer.applyAsLong(STRING_1)).thenReturn(1L);
        when(measurer.applyAsLong(STRING_2)).thenReturn(2L);
        when(measurer.applyAsLong(STRING_3)).thenReturn(3L);

        Iterator<String> trackingIterator = createTrackingIterator(Iterators.forArray(STRING_1, STRING_2, STRING_3));
        trackingIterator.forEachRemaining(_unused -> {});

        InOrder inOrder = inOrder(tracker);
        inOrder.verify(tracker).record(1L);
        inOrder.verify(tracker).record(2L);
        inOrder.verify(tracker).record(3L);
        verifyNoMoreInteractions(tracker);
    }

    @Test
    public void trackingIteratorForwardsValuesDespiteExceptionAtMeasurement() {
        when(measurer.applyAsLong(STRING_1)).thenThrow(RuntimeException.class);
        Iterator<String> trackingIterator = createTrackingIterator(Iterators.singletonIterator(STRING_1));
        assertThat(trackingIterator).toIterable().containsExactly(STRING_1);
    }

    @Test
    public void trackingIteratorForwardsValuesDespiteExceptionAtConsumption() {
        doThrow(RuntimeException.class).when(tracker).record(anyLong());
        Iterator<String> trackingIterator = createTrackingIterator(Iterators.singletonIterator(STRING_1));
        assertThat(trackingIterator).toIterable().containsExactly(STRING_1);
    }

    public Iterator<String> createTrackingIterator(Iterator<String> delegate) {
        return new TrackingIterator<>(delegate, tracker, measurer);
    }
}
