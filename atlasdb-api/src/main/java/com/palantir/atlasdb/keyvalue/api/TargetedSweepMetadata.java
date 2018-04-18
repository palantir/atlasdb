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

import org.immutables.value.Value;
import com.palantir.common.persist.Persistable;

@Value.Immutable
public abstract class TargetedSweepMetadata implements Persistable {
    public abstract boolean conservative();
    public abstract boolean dedicatedRow();
    public abstract int shard();
    public abstract int dedicatedRowNumber();

    public static final Hydrator<TargetedSweepMetadata> BYTES_HYDRATOR = input ->
            ImmutableTargetedSweepMetadata.builder()
                    .conservative((input[0] & 0x80) != 0)
                    .dedicatedRow((input[0] & 0x40) != 0)
                    .shard((input[0] << 2 | input[1] >> 6) & 0xFF)
                    .dedicatedRowNumber(input[1] & 0x3F)
                    .build();

    @Override
    public byte[] persistToBytes() {
        byte[] result = new byte[] { 0, 0, 0, 0};
        if (conservative()) {
            result[0] |= 0x80;
        }
        if (dedicatedRow()) {
            result[0] |= 0x40;
        }
        result[0] |= shard() >> 2;
        result[1] |= shard() << 6;
        result[1] |= dedicatedRowNumber();
        return result;
    }
}
