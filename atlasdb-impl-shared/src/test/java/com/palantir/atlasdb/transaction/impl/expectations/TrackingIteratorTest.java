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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Iterator;
import one.util.streamex.StreamEx;
import org.junit.Test;

public final class TrackingIteratorTest {
    @Test
    public void trackingIteratorForwardsData() {
        TrackingIterator<String, Iterator<String>> trackingIterator = new TrackingIterator<>(
                TrackingIteratorTestUtils.createStringIterator(),
                TrackingIteratorTestUtils.noOp(),
                TrackingIteratorTestUtils.STRING_MEASURER);

        assertThat(trackingIterator)
                .toIterable()
                .containsExactlyElementsOf(ImmutableList.copyOf(TrackingIteratorTestUtils.createStringIterator()));
    }

    @Test
    public void trackerInvokedCorrectlyByTrackingIterator() {
        ArrayList<Long> consumed = new ArrayList<>();
        TrackingIterator<String, Iterator<String>> trackingIterator = new TrackingIterator<>(
                TrackingIteratorTestUtils.createStringIterator(),
                consumed::add,
                TrackingIteratorTestUtils.STRING_MEASURER);
        trackingIterator.forEachRemaining(TrackingIteratorTestUtils.noOp());

        assertThat(consumed)
                .containsExactlyElementsOf(StreamEx.of(TrackingIteratorTestUtils.createStringIterator())
                        .mapToLong(TrackingIteratorTestUtils.STRING_MEASURER)
                        .boxed()
                        .toList());
    }
}
