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
package com.palantir.atlasdb.keyvalue.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

public class TargetedSweepMetadataTest {
    private static final byte[] ALL_ZERO = new byte[] {0, 0, 0, 0};
    private static final byte EIGHT_ONES = (byte) 0xFF;
    private static final byte[] ALL_ONE = new byte[] {EIGHT_ONES, EIGHT_ONES, EIGHT_ONES, EIGHT_ONES};

    private static final TargetedSweepMetadata ALL_ZERO_METADATA = ImmutableTargetedSweepMetadata.builder()
            .conservative(false)
            .dedicatedRow(false)
            .shard(0)
            .dedicatedRowNumber(0)
            .build();

    private static final TargetedSweepMetadata ALL_ONE_METADATA = ImmutableTargetedSweepMetadata.builder()
            .conservative(true)
            .dedicatedRow(true)
            .shard(1023)
            .dedicatedRowNumber(63)
            .build();

    @Test
    public void hydrateAllZero() {
        assertThat(TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(ALL_ZERO))
                .isEqualTo(ALL_ZERO_METADATA);
    }

    @Test
    public void hydrateAllOne() {
        assertThat(TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(ALL_ONE))
                .isEqualTo(ALL_ONE_METADATA);
    }

    @Test
    public void persistAndHydrateMin() {
        assertThat(TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(ALL_ZERO_METADATA.persistToBytes()))
                .isEqualTo(ALL_ZERO_METADATA);
    }

    @Test
    public void persistAndHydrateMax() {
        assertThat(TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(ALL_ONE_METADATA.persistToBytes()))
                .isEqualTo(ALL_ONE_METADATA);
    }

    @Test
    public void persistAndHydrateWhenFirstByteHasLeadingOne() {
        TargetedSweepMetadata metadata = ImmutableTargetedSweepMetadata.builder()
                .conservative(true)
                .dedicatedRow(true)
                .shard(71)
                .dedicatedRowNumber(0)
                .build();
        assertThat(TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadata.persistToBytes()))
                .isEqualTo(metadata);
    }

    @Test
    public void persistAndHydrateWhenSecondByteHasLeadingOne() {
        TargetedSweepMetadata metadata = ImmutableTargetedSweepMetadata.builder()
                .conservative(false)
                .dedicatedRow(true)
                .shard(56)
                .dedicatedRowNumber(2)
                .build();
        assertThat(TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadata.persistToBytes()))
                .isEqualTo(metadata);
    }

    @Test
    public void persistAndHydrateWith512Shards() {
        TargetedSweepMetadata metadata = ImmutableTargetedSweepMetadata.builder()
                .conservative(true)
                .dedicatedRow(true)
                .shard(512)
                .dedicatedRowNumber(1)
                .build();
        assertThat(TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadata.persistToBytes()))
                .isEqualTo(metadata);
    }

    @Test
    public void testIllegalShardSize() {
        ImmutableTargetedSweepMetadata.Builder builder =
                ImmutableTargetedSweepMetadata.builder().from(ALL_ZERO_METADATA);
        assertThatThrownBy(() -> builder.shard(3000).build()).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testIllegalDedicatedRowNumber() {
        ImmutableTargetedSweepMetadata.Builder builder =
                ImmutableTargetedSweepMetadata.builder().from(ALL_ZERO_METADATA);
        assertThatThrownBy(() -> builder.dedicatedRowNumber(-1).build()).isInstanceOf(IllegalArgumentException.class);
    }
}
