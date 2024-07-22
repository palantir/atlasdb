/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.asts.progress;

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.sweep.queue.SweepQueueUtils;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import org.junit.jupiter.api.Test;

public final class BucketProgressTest {
    private static final long TIMESTAMP_OFFSET_1 = 7L;
    private static final long TIMESTAMP_OFFSET_2 = 777L;
    private static final long CELL_OFFSET_1 = 42L;
    private static final long CELL_OFFSET_2 = 9999L;

    @Test
    public void createForTimestampOffsetReturnsProgressWithMatchingTimestampOffset() {
        assertThat(BucketProgress.createForTimestampOffset(TIMESTAMP_OFFSET_1).timestampOffset())
                .isEqualTo(TIMESTAMP_OFFSET_1);
        assertThat(BucketProgress.createForTimestampOffset(TIMESTAMP_OFFSET_2).timestampOffset())
                .isEqualTo(TIMESTAMP_OFFSET_2);
    }

    @Test
    public void createForTimestampOffsetReturnsProgressWithZeroCellOffset() {
        assertThat(BucketProgress.createForTimestampOffset(TIMESTAMP_OFFSET_1).cellOffset())
                .isEqualTo(0L);
        assertThat(BucketProgress.createForTimestampOffset(TIMESTAMP_OFFSET_2).cellOffset())
                .isEqualTo(0L);
    }

    @Test
    public void cannotCreateWithNegativeTimestampOffset() {
        assertThatLoggableExceptionThrownBy(() -> BucketProgress.createForTimestampOffset(-CELL_OFFSET_1))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasExactlyArgs(SafeArg.of("timestampOffset", -CELL_OFFSET_1));
    }

    @Test
    public void cannotCreateWithNegativeCellOffset() {
        assertThatLoggableExceptionThrownBy(() -> ImmutableBucketProgress.builder()
                        .timestampOffset(TIMESTAMP_OFFSET_1)
                        .cellOffset(-CELL_OFFSET_1)
                        .build())
                .isInstanceOf(SafeIllegalStateException.class)
                .hasExactlyArgs(SafeArg.of("cellOffset", -CELL_OFFSET_1));
    }

    @Test
    public void cannotCreateWithTimestampOffsetGreaterThanFinePartition() {
        assertThatLoggableExceptionThrownBy(
                        () -> BucketProgress.createForTimestampOffset(SweepQueueUtils.TS_FINE_GRANULARITY))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasExactlyArgs(SafeArg.of("timestampOffset", SweepQueueUtils.TS_FINE_GRANULARITY));
        assertThatLoggableExceptionThrownBy(() -> BucketProgress.createForTimestampOffset(Long.MAX_VALUE))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasExactlyArgs(SafeArg.of("timestampOffset", Long.MAX_VALUE));
    }

    @Test
    public void sameTimestampAndCellOffsetComparesAsEqual() {
        BucketProgress progressOne = BucketProgress.builder()
                .timestampOffset(TIMESTAMP_OFFSET_1)
                .cellOffset(CELL_OFFSET_1)
                .build();
        BucketProgress progressTwo = BucketProgress.builder()
                .timestampOffset(TIMESTAMP_OFFSET_1)
                .cellOffset(CELL_OFFSET_1)
                .build();

        assertThat(progressOne).isNotSameAs(progressTwo);
        assertThat(progressOne.compareTo(progressTwo)).isEqualTo(0L);
        assertThat(progressTwo.compareTo(progressOne)).isEqualTo(0L);
    }

    @Test
    public void lowerTimestampOffsetComparesAsLesser() {
        BucketProgress progressOne = BucketProgress.builder()
                .timestampOffset(TIMESTAMP_OFFSET_1)
                .cellOffset(CELL_OFFSET_1)
                .build();
        BucketProgress progressTwo = BucketProgress.builder()
                .timestampOffset(TIMESTAMP_OFFSET_2)
                .cellOffset(CELL_OFFSET_1)
                .build();

        assertThat(progressOne.compareTo(progressTwo)).isNegative();
        assertThat(progressTwo.compareTo(progressOne)).isPositive();
    }

    @Test
    public void lowerCellOffsetWithMatchingTimestampComparesAsLesser() {
        BucketProgress progressOne = BucketProgress.builder()
                .timestampOffset(TIMESTAMP_OFFSET_1)
                .cellOffset(CELL_OFFSET_1)
                .build();
        BucketProgress progressTwo = BucketProgress.builder()
                .timestampOffset(TIMESTAMP_OFFSET_1)
                .cellOffset(CELL_OFFSET_2)
                .build();

        assertThat(progressOne.compareTo(progressTwo)).isNegative();
        assertThat(progressTwo.compareTo(progressOne)).isPositive();
    }

    @Test
    public void lowerTimestampOffsetAndHigherCellOffsetComparesAsLesser() {
        BucketProgress progressOne = BucketProgress.builder()
                .timestampOffset(TIMESTAMP_OFFSET_1)
                .cellOffset(CELL_OFFSET_2)
                .build();
        BucketProgress progressTwo = BucketProgress.builder()
                .timestampOffset(TIMESTAMP_OFFSET_2)
                .cellOffset(CELL_OFFSET_1)
                .build();

        assertThat(progressOne.compareTo(progressTwo)).isNegative();
        assertThat(progressTwo.compareTo(progressOne)).isPositive();
    }
}
