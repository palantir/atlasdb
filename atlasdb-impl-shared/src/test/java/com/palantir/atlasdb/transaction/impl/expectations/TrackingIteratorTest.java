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

import static org.assertj.core.api.Assertions.assertThatIterator;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Iterator;
import java.util.function.Consumer;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class TrackingIteratorTest extends AbstractTrackingIteratorTest {
    @Test
    public void trackingIteratorForwardsData() {
        TrackingIterator<String, Iterator<String>> trackingIterator =
                new TrackingIterator<>(createStringIterator(), noOp(), STRING_MEASURER);
        assertThatIterator(trackingIterator).toIterable().containsExactlyElementsOf(STRINGS);
    }

    @Test
    public void trackerInvokedCorrectlyByTrackingIterator() {
        Consumer<Long> tracker = spy(noOp());
        InOrder inOrder = Mockito.inOrder(tracker);

        TrackingIterator<String, Iterator<String>> trackingIterator =
                new TrackingIterator<>(createStringIterator(), tracker, STRING_MEASURER);

        trackingIterator.forEachRemaining(string -> {
            inOrder.verify(tracker).accept(STRING_MEASURER.apply(string));
        });

        verifyNoMoreInteractions(tracker);
    }
}
