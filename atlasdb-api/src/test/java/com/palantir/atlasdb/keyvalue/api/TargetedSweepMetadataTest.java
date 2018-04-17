/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.keyvalue.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class TargetedSweepMetadataTest {
    private static final byte[] ALL_ZERO = new byte[] { 0, 0, 0, 0 };
    private static final byte[] ALL_ONE = new byte[] { -1, -1, -1, -1 };

    private static final TargetedSweepMetadata MIN_METADATA = ImmutableTargetedSweepMetadata.builder()
            .conservative(false)
            .dedicatedRow(false)
            .shard(0)
            .dedicatedRowNumber(0)
            .build();

    private static final TargetedSweepMetadata MAX_METADATA = ImmutableTargetedSweepMetadata.builder()
            .conservative(true)
            .dedicatedRow(true)
            .shard(255)
            .dedicatedRowNumber(63)
            .build();

    @Test
    public void hydrateAllZero() {
        assertThat(TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(ALL_ZERO)).isEqualTo(MIN_METADATA);
    }

    @Test
    public void hydrateAllOne() {
        assertThat(TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(ALL_ONE)).isEqualTo(MAX_METADATA);
    }

    @Test
    public void persistAndHydrateMin() {
        assertThat(TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(MIN_METADATA.persistToBytes()))
                .isEqualTo(MIN_METADATA);
    }

    @Test
    public void persistAndHydrateMax() {
        assertThat(TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(MAX_METADATA.persistToBytes()))
                .isEqualTo(MAX_METADATA);
    }

    @Test
    public void persistAndHydrate() {
        TargetedSweepMetadata metadata = ImmutableTargetedSweepMetadata.builder()
                .conservative(false)
                .dedicatedRow(true)
                .shard(196)
                .dedicatedRowNumber(17)
                .build();
        assertThat(TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadata.persistToBytes()))
                .isEqualTo(metadata);
    }
}
