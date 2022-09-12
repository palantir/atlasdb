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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

public class TargetedSweepMetadataTest {
    private static final byte[] ALL_ZERO = new byte[] {0, 0, 0, 0};
    private static final byte EIGHT_ONES = (byte) 0xFF;
    private static final byte[] MOSTLY_ONE = new byte[] {EIGHT_ONES, EIGHT_ONES, 0, EIGHT_ONES};

    private static final TargetedSweepMetadata ALL_ZERO_METADATA = ImmutableTargetedSweepMetadata.builder()
            .conservative(false)
            .dedicatedRow(false)
            .shard(0)
            .dedicatedRowNumber(0)
            .nonSweepableTransaction(false)
            .build();

    private static final TargetedSweepMetadata MOSTLY_ONE_METADATA = ImmutableTargetedSweepMetadata.builder()
            .conservative(true)
            .dedicatedRow(true)
            .shard(255)
            .dedicatedRowNumber(63)
            .nonSweepableTransaction(false)
            .build();

    @Test
    public void hydrateAllZero() {
        assertThat(TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(ALL_ZERO))
                .isEqualTo(ALL_ZERO_METADATA);
    }

    @Test
    public void hydrateAllOne() {
        assertThat(TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(MOSTLY_ONE))
                .isEqualTo(MOSTLY_ONE_METADATA);
    }

    @Test
    public void persistAndHydrateMin() {
        assertThat(TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(ALL_ZERO_METADATA.persistToBytes()))
                .isEqualTo(ALL_ZERO_METADATA);
    }

    @Test
    public void persistAndHydrateMax() {
        assertThat(TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(MOSTLY_ONE_METADATA.persistToBytes()))
                .isEqualTo(MOSTLY_ONE_METADATA);
    }

    @Test
    public void persistAndHydrateWhenFirstByteHasLeadingOne() {
        TargetedSweepMetadata metadata = ImmutableTargetedSweepMetadata.builder()
                .conservative(true)
                .dedicatedRow(false)
                .shard(71)
                .dedicatedRowNumber(0)
                .nonSweepableTransaction(false)
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
                .nonSweepableTransaction(false)
                .build();
        assertThat(TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadata.persistToBytes()))
                .isEqualTo(metadata);
    }

    @Test
    public void testIllegalShardSize() {
        ImmutableTargetedSweepMetadata.Builder builder =
                ImmutableTargetedSweepMetadata.builder().from(ALL_ZERO_METADATA);
        assertThatThrownBy(builder.shard(300)::build).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testIllegalDedicatedRowNumber() {
        ImmutableTargetedSweepMetadata.Builder builder =
                ImmutableTargetedSweepMetadata.builder().from(ALL_ZERO_METADATA);
        assertThatThrownBy(builder.dedicatedRowNumber(-1)::build).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testNonSweepable() {
        assertThatThrownBy(defaultBuilder().nonSweepableTransaction(true)::build)
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        defaultBuilder().conservative(true).dedicatedRow(true).nonSweepableTransaction(true)::build)
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(defaultBuilder().conservative(true).shard(1).nonSweepableTransaction(true)::build)
                .isInstanceOf(IllegalArgumentException.class);
        assertThatCode(defaultBuilder().conservative(true).nonSweepableTransaction(true)::build)
                .doesNotThrowAnyException();
    }

    @Test
    public void serializeDeserializeNonSweepable() {
        TargetedSweepMetadata nonSweepableMetadata = defaultBuilder()
                .conservative(true)
                .nonSweepableTransaction(true)
                .build();
        assertThat(TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(nonSweepableMetadata.persistToBytes()))
                .isEqualTo(nonSweepableMetadata);
    }

    private static ImmutableTargetedSweepMetadata.Builder defaultBuilder() {
        return ImmutableTargetedSweepMetadata.builder().from(ALL_ZERO_METADATA);
    }
}
