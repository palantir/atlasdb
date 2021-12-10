/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.internalschema;

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import org.junit.Test;

public class TimestampPartitioningMapTest {
    private static final TimestampPartitioningMap<Integer> DEFAULT_INITIAL_MAPPING =
            TimestampPartitioningMap.of(ImmutableRangeMap.of(Range.atLeast(1L), 1));

    private static final long TIMESTAMP_1 = 77L;
    private static final long TIMESTAMP_2 = 777L;
    private static final long TIMESTAMP_3 = 7777L;

    @Test
    public void throwsIfInitialMapIsEmpty() {
        assertThatLoggableExceptionThrownBy(() -> TimestampPartitioningMap.of(ImmutableRangeMap.of()))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("its span does not cover precisely all timestamps");
    }

    @Test
    public void throwsIfInitialMapDoesNotCoverFullRange() {
        assertThatLoggableExceptionThrownBy(
                        () -> TimestampPartitioningMap.of(ImmutableRangeMap.of(Range.atLeast(42L), 1)))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("its span does not cover precisely all timestamps");
    }

    @Test
    public void throwsIfInitialMapHasGapsInRange() {
        assertThatLoggableExceptionThrownBy(() -> TimestampPartitioningMap.of(ImmutableRangeMap.<Long, Integer>builder()
                        .put(Range.closed(1L, 6L), 1)
                        .put(Range.atLeast(8L), 2)
                        .build()))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("While the span covers all timestamps, some are disconnected.");
    }

    @Test
    public void throwsIfInitialMapExceedsTimestampRange() {
        assertThatLoggableExceptionThrownBy(
                        () -> TimestampPartitioningMap.of(ImmutableRangeMap.of(Range.atLeast(-42L), 1)))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("its span does not cover precisely all timestamps");
    }

    @Test
    public void getsVersionForTimestamp() {
        assertThat(DEFAULT_INITIAL_MAPPING.getValueForTimestamp(68)).isEqualTo(1);
    }

    @Test
    public void copiesWithNewVersions() {
        TimestampPartitioningMap<Integer> newMap = DEFAULT_INITIAL_MAPPING.copyInstallingNewValue(TIMESTAMP_1, 2);
        assertThat(newMap.getValueForTimestamp(TIMESTAMP_1 - 1)).isEqualTo(1);
        assertThat(newMap.getValueForTimestamp(TIMESTAMP_1)).isEqualTo(2);
        assertThat(newMap.getValueForTimestamp(TIMESTAMP_2)).isEqualTo(2);
    }

    @Test
    public void supportsRevertingVersions() {
        TimestampPartitioningMap<Integer> newMap =
                DEFAULT_INITIAL_MAPPING.copyInstallingNewValue(TIMESTAMP_1, 2).copyInstallingNewValue(TIMESTAMP_2, 1);
        assertThat(newMap.getValueForTimestamp(TIMESTAMP_2 - 1)).isEqualTo(2);
        assertThat(newMap.getValueForTimestamp(TIMESTAMP_2)).isEqualTo(1);
    }

    @Test
    public void coalescesRangesOnEquivalentValues() {
        TimestampPartitioningMap<Integer> newMap = DEFAULT_INITIAL_MAPPING
                .copyInstallingNewValue(TIMESTAMP_1, 2)
                .copyInstallingNewValue(TIMESTAMP_2, 2)
                .copyInstallingNewValue(TIMESTAMP_3, 2);
        assertThat(newMap.rangeMapView().asMapOfRanges()).hasSize(2);
    }

    @Test
    public void doesNotCoalesceRangesOnDifferentValues() {
        TimestampPartitioningMap<Integer> newMap = DEFAULT_INITIAL_MAPPING
                .copyInstallingNewValue(TIMESTAMP_1, 2)
                .copyInstallingNewValue(TIMESTAMP_2, 1)
                .copyInstallingNewValue(TIMESTAMP_3, 2);
        assertThat(newMap.rangeMapView().asMapOfRanges()).hasSize(4);
    }

    @Test
    public void throwsWhenInstallingVersionInThePast() {
        TimestampPartitioningMap<Integer> newMap = DEFAULT_INITIAL_MAPPING.copyInstallingNewValue(TIMESTAMP_2, 2);
        assertThatThrownBy(() -> newMap.copyInstallingNewValue(TIMESTAMP_1, 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot install a new value at an earlier timestamp");
    }
}
