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

import com.google.common.base.Preconditions;
import com.palantir.common.persist.Persistable;
import org.immutables.value.Value;

@Value.Immutable
public abstract class TargetedSweepMetadata implements Persistable {
    public abstract boolean conservative();

    public abstract boolean dedicatedRow();

    public abstract int shard();

    public abstract long dedicatedRowNumber();

    public static final int MAX_SHARDS = 1024;
    public static final int MAX_DEDICATED_ROWS = 64;
    private static final int SWEEP_STRATEGY_MASK = 0x80;
    private static final int USE_DEDICATED_ROWS_MASK = 0x40;
    private static final int DEDICATED_ROW_NUMBER_MASK = 0x3F;
    private static final int EXTRA_SHARD_MASK = 0xC0;
    private static final int BYTE_MASK = 0xFF;

    @Value.Check
    void checkShardSize() {
        Preconditions.checkArgument(
                shard() >= 0 && shard() < MAX_SHARDS,
                "Shard number must non-negative and strictly less than %s, but it is %s.",
                MAX_SHARDS,
                shard());
    }

    @Value.Check
    void checkRowNumber() {
        Preconditions.checkArgument(
                dedicatedRowNumber() >= 0 && dedicatedRowNumber() < MAX_DEDICATED_ROWS,
                "Dedicated row number must non-negative and strictly less than %s, but it is %s.",
                MAX_DEDICATED_ROWS,
                dedicatedRowNumber());
    }

    public static final Hydrator<TargetedSweepMetadata> BYTES_HYDRATOR =
            input -> ImmutableTargetedSweepMetadata.builder()
                    .conservative((input[0] & SWEEP_STRATEGY_MASK) != 0)
                    .dedicatedRow((input[0] & USE_DEDICATED_ROWS_MASK) != 0)
                    .shard(((input[2] & EXTRA_SHARD_MASK) << 2)
                            + ((input[0] << 2) & BYTE_MASK)
                            + ((input[1] & BYTE_MASK) >> 6))
                    .dedicatedRowNumber(input[1] & DEDICATED_ROW_NUMBER_MASK)
                    .build();

    @Override
    public byte[] persistToBytes() {
        byte[] result = new byte[] {0, 0, 0, 0};
        result[0] |= (shard() & BYTE_MASK) >> 2;
        if (dedicatedRow()) {
            result[0] |= USE_DEDICATED_ROWS_MASK;
        }
        if (conservative()) {
            result[0] |= SWEEP_STRATEGY_MASK;
        }
        result[1] |= dedicatedRowNumber() & DEDICATED_ROW_NUMBER_MASK;
        result[1] |= (shard() << 6) & BYTE_MASK;
        result[2] |= (shard() >> 8 << 6) & BYTE_MASK;
        return result;
    }
}
