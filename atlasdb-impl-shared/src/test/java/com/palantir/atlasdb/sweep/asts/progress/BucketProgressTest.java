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
    private static final long TIMESTAMP_1 = 7L;
    private static final long TIMESTAMP_2 = 777L;

    @Test
    public void createForTimestampOffsetReturnsProgressWithZeroCellOffset() {
        assertThat(BucketProgress.createForTimestampOffset(TIMESTAMP_1))
                .isEqualTo(ImmutableBucketProgress.builder()
                        .timestampOffset(TIMESTAMP_1)
                        .cellOffset(0L)
                        .build());
        assertThat(BucketProgress.createForTimestampOffset(TIMESTAMP_2))
                .isEqualTo(ImmutableBucketProgress.builder()
                        .timestampOffset(TIMESTAMP_2)
                        .cellOffset(0L)
                        .build());
    }

    @Test
    public void cannotCreateWithNegativeTimestampOffset() {
        assertThatLoggableExceptionThrownBy(() -> BucketProgress.createForTimestampOffset(-42L))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasExactlyArgs(SafeArg.of("timestampOffset", -42L));
    }

    @Test
    public void cannotCreateWithNegativeCellOffset() {
        assertThatLoggableExceptionThrownBy(() -> ImmutableBucketProgress.builder()
                        .timestampOffset(TIMESTAMP_1)
                        .cellOffset(-55L)
                        .build())
                .isInstanceOf(SafeIllegalStateException.class)
                .hasExactlyArgs(SafeArg.of("cellOffset", -55L));
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
}
