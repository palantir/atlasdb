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

package com.palantir.atlasdb.transaction.knowledge;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import java.util.Optional;
import org.junit.Test;

@SuppressWarnings("UnstableApiUsage") // RangeSet usage
public class KnownConcludedTransactionsStoreTest {

    private static final Optional<Long> MINIMUM_TIMESTAMP = Optional.of(25L);
    private final KeyValueService keyValueService = new InMemoryKeyValueService(true);
    private final KnownConcludedTransactionsStore knownConcludedTransactionsStore =
            KnownConcludedTransactionsStore.create(keyValueService);

    @Test
    public void storeBeginsEmpty() {
        assertThat(knownConcludedTransactionsStore.get()).isEmpty();
    }

    @Test
    public void canRetrieveStoredRange() {
        knownConcludedTransactionsStore.supplement(Range.closedOpen(1L, 100L));
        assertThat(knownConcludedTransactionsStore.get())
                .contains(TimestampRangeSet.singleRange(Range.closedOpen(1L, 100L), 0L));
    }

    @Test
    public void coalescesRangesBeforeStorage() {
        knownConcludedTransactionsStore.supplement(Range.closedOpen(1L, 100L));
        knownConcludedTransactionsStore.supplement(Range.closedOpen(50L, 200L));
        assertThat(knownConcludedTransactionsStore.get())
                .contains(TimestampRangeSet.singleRange(Range.closedOpen(1L, 200L), 0L));
    }

    @Test
    public void tracksDistinctRanges() {
        knownConcludedTransactionsStore.supplement(Range.closedOpen(1L, 100L));
        knownConcludedTransactionsStore.supplement(Range.closedOpen(150L, 200L));
        assertThat(knownConcludedTransactionsStore.get())
                .contains(ImmutableTimestampRangeSet.builder()
                        .timestampRanges(ImmutableRangeSet.<Long>builder()
                                .add(Range.closedOpen(1L, 100L))
                                .add(Range.closedOpen(150L, 200L))
                                .build())
                        .minimumTimestamp(0L)
                        .build());
    }

    @Test
    public void rangesThatContainMinimumTimestampModifiedToHaveMinimumTimestampAsLowerBound() {
        Range<Long> range = Range.closed(10L, 100L);
        Range<Long> modifiedRange =
                knownConcludedTransactionsStore.maybeModifyRangeLowerBoundToMinimumTimestamp(MINIMUM_TIMESTAMP, range);
        assertThat(modifiedRange.lowerEndpoint()).isEqualTo(MINIMUM_TIMESTAMP.get());
        assertThat(modifiedRange.upperEndpoint()).isEqualTo(range.upperEndpoint());
    }

    @Test
    public void rangesThatDoesNotContainMinimumTimestampShouldNotHaveLowerBoundModified() {
        Range<Long> range = Range.closed(50L, 100L);
        Range<Long> maybeModifiedRange =
                knownConcludedTransactionsStore.maybeModifyRangeLowerBoundToMinimumTimestamp(MINIMUM_TIMESTAMP, range);
        assertThat(maybeModifiedRange.lowerEndpoint()).isEqualTo(range.lowerEndpoint());
        assertThat(maybeModifiedRange.upperEndpoint()).isEqualTo(range.upperEndpoint());
    }

    @Test
    public void rangesAreNotModifiedIfMinimumTimestampEmpty() {
        Range<Long> range = Range.closed(10L, 100L);
        Range<Long> maybeModifiedRange =
                knownConcludedTransactionsStore.maybeModifyRangeLowerBoundToMinimumTimestamp(Optional.empty(), range);
        assertThat(maybeModifiedRange.lowerEndpoint()).isEqualTo(range.lowerEndpoint());
        assertThat(maybeModifiedRange.upperEndpoint()).isEqualTo(range.upperEndpoint());
    }
}
