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

package com.palantir.atlasdb.cleaner.external;

import com.google.common.hash.Hashing;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.schema.cleanup.StreamStoreCleanupMetadata;
import com.palantir.atlasdb.table.description.ValueType;

public final class StreamStoreHashEncodingUtils {
    private StreamStoreHashEncodingUtils() {
        // utility class
    }

    // TODO (jkong): Sort out duplication between this and getValueHashComponent()
    public static byte[] getGeneralHashComponent(StreamStoreCleanupMetadata cleanupMetadata, long streamId) {
        switch (cleanupMetadata.numHashedRowComponents()) {
            case 0:
                return PtBytes.EMPTY_BYTE_ARRAY;
            case 1:
            case 2:
                return computeHashFirstComponent(cleanupMetadata.streamIdType(), streamId);
            default:
                throw new IllegalStateException("Unexpected number of hashed components: " +
                        cleanupMetadata.numHashedRowComponents());
        }
    }

    public static byte[] getValueHashComponent(
            StreamStoreCleanupMetadata cleanupMetadata, long streamId, long blockId) {
        switch (cleanupMetadata.numHashedRowComponents()) {
            case 0:
                return PtBytes.EMPTY_BYTE_ARRAY;
            case 1:
                return computeHashFirstComponent(cleanupMetadata.streamIdType(), streamId);
            case 2:
                return computeHashFirstComponents(cleanupMetadata.streamIdType(), streamId, blockId);
            default:
                throw new IllegalStateException("Unexpected number of hashed components: " +
                        cleanupMetadata.numHashedRowComponents());
        }
    }

    private static byte[] computeHashFirstComponent(ValueType streamIdType, long streamId) {
        byte[] streamIdBytes = streamIdType.convertFromJava(streamId);
        return applyBitwiseXorWithMinValueAndConvert(computeMurmurHash(streamIdBytes));
    }

    private static byte[] computeHashFirstComponents(ValueType streamIdType, long streamId, long blockId) {
        byte[] streamIdBytes = streamIdType.convertFromJava(streamId);
        // This is always VAR_LONG, regardless of the stream id type.
        // See StreamTableDefinitionBuilder#build() for case VALUE
        byte[] blockIdBytes = EncodingUtils.encodeUnsignedVarLong(blockId);
        return applyBitwiseXorWithMinValueAndConvert(computeMurmurHash(streamIdBytes, blockIdBytes));
    }

    private static byte[] applyBitwiseXorWithMinValueAndConvert(long input) {
        return PtBytes.toBytes(Long.MIN_VALUE ^ input);
    }

    private static long computeMurmurHash(byte[]... bytes) {
        return Hashing.murmur3_128().hashBytes(EncodingUtils.add(bytes)).asLong();
    }
}
