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
import org.junit.Test;

/**
 * Tests on this class are fairly lightweight, as they focus primarily on correct deferral to the range-set
 * underlying a {@link TimestampRangeSet}.
 */
@SuppressWarnings("UnstableApiUsage") // RangeSet
public class TimestampRangeSetTest {
    private static final ImmutableRangeSet<Long> BASE_RANGES = ImmutableRangeSet.<Long>builder()
            .add(Range.openClosed(10L, 20L))
            .add(Range.openClosed(30L, 40L))
            .build();
    private static final TimestampRangeSet BASE_RANGE_SET =
            ImmutableTimestampRangeSet.builder().timestampRanges(BASE_RANGES).build();

    @Test
    public void enclosesIdentifiesDirectMatch() {
        assertThat(BASE_RANGE_SET.encloses(Range.openClosed(10L, 20L))).isTrue();
    }

    @Test
    public void enclosesIdentifiesSubRanges() {
        assertThat(BASE_RANGE_SET.encloses(Range.openClosed(10L, 17L))).isTrue();
        assertThat(BASE_RANGE_SET.encloses(Range.openClosed(13L, 17L))).isTrue();
        assertThat(BASE_RANGE_SET.encloses(Range.openClosed(13L, 20L))).isTrue();
    }

    @Test
    public void enclosesChecksRangeEndpointTypes() {
        assertThat(BASE_RANGE_SET.encloses(Range.closedOpen(10L, 20L))).isFalse();
        assertThat(BASE_RANGE_SET.encloses(Range.closed(10L, 20L))).isFalse();
        assertThat(BASE_RANGE_SET.encloses(Range.open(10L, 20L))).isTrue();
    }

    @Test
    public void enclosesReturnsFalseForRangesOutsideSet() {
        assertThat(BASE_RANGE_SET.encloses(Range.openClosed(0L, 2L))).isFalse();
        assertThat(BASE_RANGE_SET.encloses(Range.closedOpen(66L, 94L))).isFalse();
        assertThat(BASE_RANGE_SET.encloses(Range.openClosed(1L, 20L))).isFalse();
    }

    @Test
    public void copyAndAddWorksWithEmptyRange() {
        Range<Long> newRange = Range.openClosed(3L, 71L);
        assertThat(TimestampRangeSet.empty().copyAndAdd(newRange)).isEqualTo(TimestampRangeSet.singleRange(newRange));
    }

    @Test
    public void copyAndAddPreservesExistingRangesIfEnclosing() {
        assertThat(BASE_RANGE_SET.copyAndAdd(Range.openClosed(33L, 38L))).isEqualTo(BASE_RANGE_SET);
    }

    @Test
    public void copyAndAddExtendsExistingRangesIfOverlapping() {
        assertThat(BASE_RANGE_SET.copyAndAdd(Range.openClosed(35L, 55L)).timestampRanges())
                .isEqualTo(ImmutableRangeSet.<Long>builder()
                        .add(Range.openClosed(10L, 20L))
                        .add(Range.openClosed(30L, 55L))
                        .build());
    }

    @Test
    public void copyAndAddAddsSeparateRangeIfDisjoint() {
        Range<Long> outsideRange = Range.openClosed(50L, 55L);
        assertThat(BASE_RANGE_SET.copyAndAdd(outsideRange).timestampRanges())
                .isEqualTo(ImmutableRangeSet.<Long>builder()
                        .addAll(BASE_RANGES)
                        .add(outsideRange)
                        .build());
    }
}
