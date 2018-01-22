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

package com.palantir.atlasdb.sweep.external;

import com.google.common.hash.Hashing;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.ptobject.EncodingUtils;

public final class StreamStoreHashEncodingUtils {
    private StreamStoreHashEncodingUtils() {
        // utility class
    }

    // TODO (jkong): Sort out duplication between this and getValueHashComponent()
    public static byte[] getGeneralHashComponent(int numHashedRowComponents, byte[] streamId) {
        switch (numHashedRowComponents) {
            case 0:
                return PtBytes.EMPTY_BYTE_ARRAY;
            case 1:
            case 2:
                return computeHashFirstComponent(streamId);
            default:
                throw new IllegalStateException("Unexpected number of hashed components: " +
                        numHashedRowComponents);
        }
    }

    public static byte[] getValueHashComponent(
            int numHashedRowComponents, byte[] streamId, long blockId) {
        switch (numHashedRowComponents) {
            case 0:
                return PtBytes.EMPTY_BYTE_ARRAY;
            case 1:
                return computeHashFirstComponent(streamId);
            case 2:
                return computeHashFirstComponents(streamId, blockId);
            default:
                throw new IllegalStateException("Unexpected number of hashed components: " +
                        numHashedRowComponents);
        }
    }

    private static byte[] computeHashFirstComponent(byte[] streamId) {
        return applyBitwiseXorWithMinValueAndConvert(computeMurmurHash(streamId));
    }

    private static byte[] computeHashFirstComponents(byte[] streamId, long blockId) {
        // This is always VAR_LONG, regardless of the stream id type.
        // See StreamTableDefinitionBuilder#build() for case VALUE
        byte[] blockIdBytes = EncodingUtils.encodeUnsignedVarLong(blockId);
        return applyBitwiseXorWithMinValueAndConvert(computeMurmurHash(streamId, blockIdBytes));
    }

    private static byte[] applyBitwiseXorWithMinValueAndConvert(long input) {
        return PtBytes.toBytes(Long.MIN_VALUE ^ input);
    }

    private static long computeMurmurHash(byte[]... bytes) {
        return Hashing.murmur3_128().hashBytes(EncodingUtils.add(bytes)).asLong();
    }
}
