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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public final class BucketProgressTest {
    private static final long TIMESTAMP_1 = 7L;
    private static final long TIMESTAMP_2 = 7777777L;

    @Test
    public void createForTimestampReturnsProgressWithZeroCellOffset() {
        assertThat(BucketProgress.createForTimestamp(TIMESTAMP_1))
                .isEqualTo(ImmutableBucketProgress.builder()
                        .timestampOffset(TIMESTAMP_1)
                        .cellOffset(0L)
                        .build());
        assertThat(BucketProgress.createForTimestamp(TIMESTAMP_2))
                .isEqualTo(ImmutableBucketProgress.builder()
                        .timestampOffset(TIMESTAMP_2)
                        .cellOffset(0L)
                        .build());
    }
}
