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
import static org.assertj.core.api.Assertions.assertThatCode;

import com.palantir.atlasdb.sweep.queue.SweepQueueUtils;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import org.junit.jupiter.api.Test;

public final class BucketProgressTest {
    private static final long NO_PROGRESS = -1L;
    private static final long TIMESTAMP_1 = 7L;
    private static final long TIMESTAMP_2 = 777L;
    private static final long CELL_PROGRESS_1 = 25L;
    private static final long CELL_PROGRESS_2 = SweepQueueUtils.TS_FINE_GRANULARITY - 1;

    @Test
    public void createForTimestampProgressReturnsProgressWithNoCellProgressForNextTimestamp() {
        assertThat(BucketProgress.createForTimestampProgress(TIMESTAMP_1))
                .isEqualTo(ImmutableBucketProgress.builder()
                        .timestampProgress(TIMESTAMP_1)
                        .cellProgressForNextTimestamp(-1L)
                        .build());
        assertThat(BucketProgress.createForTimestampProgress(TIMESTAMP_2))
                .isEqualTo(ImmutableBucketProgress.builder()
                        .timestampProgress(TIMESTAMP_2)
                        .cellProgressForNextTimestamp(-1L)
                        .build());
    }

    @Test
    public void canCreateWithPositiveCellProgress() {
        assertCanCreateWith(TIMESTAMP_1, CELL_PROGRESS_1);
        assertCanCreateWith(TIMESTAMP_2, CELL_PROGRESS_2);
    }

    @Test
    public void canCreateWithTimestampProgressWithoutCellProgress() {
        assertCanCreateWith(TIMESTAMP_1, NO_PROGRESS);
        assertCanCreateWith(TIMESTAMP_2, NO_PROGRESS);
    }

    @Test
    public void canCreateWithCellProgressWithoutTimestampProgress() {
        assertCanCreateWith(NO_PROGRESS, CELL_PROGRESS_1);
        assertCanCreateWith(NO_PROGRESS, CELL_PROGRESS_2);
    }

    @Test
    public void canCreateWithNoProgress() {
        assertCanCreateWith(NO_PROGRESS, NO_PROGRESS);
    }

    @Test
    public void cannotCreateWithNegativeTimestampProgressOtherThanNegativeOne() {
        assertThatLoggableExceptionThrownBy(() -> BucketProgress.createForTimestampProgress(-42L))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasExactlyArgs(SafeArg.of("timestampProgress", -42L));
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored") // Validating that an exception is thrown
    public void cannotCreateWithNegativeCellProgressOtherThanNegativeOne() {
        assertThatLoggableExceptionThrownBy(() -> ImmutableBucketProgress.builder()
                        .timestampProgress(TIMESTAMP_1)
                        .cellProgressForNextTimestamp(-55L)
                        .build())
                .isInstanceOf(SafeIllegalStateException.class)
                .hasExactlyArgs(SafeArg.of("cellProgressForNextTimestamp", -55L));
    }

    @Test
    public void canCreateWithTimestampProgressGreaterThanSweepQueuePartitions() {
        assertCanCreateWith(SweepQueueUtils.TS_FINE_GRANULARITY + 1, NO_PROGRESS);
        assertCanCreateWith(SweepQueueUtils.TS_COARSE_GRANULARITY + 1, NO_PROGRESS);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored") // Validating that no exception is thrown
    private void assertCanCreateWith(long timestampProgress, long cellProgressForNextTimestamp) {
        assertThatCode(() -> BucketProgress.builder()
                        .timestampProgress(timestampProgress)
                        .cellProgressForNextTimestamp(cellProgressForNextTimestamp)
                        .build())
                .doesNotThrowAnyException();
    }
}
