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

import static com.palantir.atlasdb.transaction.impl.expectations.TrackingIteratorTestUtils.STRING_MEASURER;
import static com.palantir.atlasdb.transaction.impl.expectations.TrackingIteratorTestUtils.consumeIteratorIntoList;
import static com.palantir.atlasdb.transaction.impl.expectations.TrackingIteratorTestUtils.createStringIterator;
import static com.palantir.atlasdb.transaction.impl.expectations.TrackingIteratorTestUtils.noOp;
import static org.assertj.core.api.Assertions.assertThatIterable;
import static org.assertj.core.api.Assertions.assertThatIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Collectors;
import org.junit.Test;

public final class TrackingIteratorTest {
    @Test
    public void trackingIteratorForwardsData() {
        TrackingIterator<String, Iterator<String>> trackingIterator =
                new TrackingIterator<>(createStringIterator(), noOp(), STRING_MEASURER);

        assertThatIterator(trackingIterator)
                .toIterable()
                .containsExactlyElementsOf(consumeIteratorIntoList(createStringIterator()));
    }

    @Test
    public void trackerInvokedCorrectlyByTrackingIterator() {
        ArrayList<Long> consumed = new ArrayList<>();
        TrackingIterator<String, Iterator<String>> trackingIterator =
                new TrackingIterator<>(createStringIterator(), consumed::add, STRING_MEASURER);
        trackingIterator.forEachRemaining(noOp());

        assertThatIterable(consumed)
                .containsExactlyElementsOf(consumeIteratorIntoList(createStringIterator()).stream()
                        .mapToLong(STRING_MEASURER)
                        .boxed()
                        .collect(Collectors.toUnmodifiableList()));
    }
}
