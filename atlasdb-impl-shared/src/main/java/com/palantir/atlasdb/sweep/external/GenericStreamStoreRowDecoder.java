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

import java.util.Arrays;

import com.palantir.atlasdb.schema.cleanup.StreamStoreCleanupMetadata;

/**
 * This class decodes row components of a stream store into a {@link GenericStreamIdentifier}, without needing to know
 * the stream store's schema.
 *
 * Note that of the {@link com.palantir.atlasdb.schema.stream.StreamTableType}s, we only currently support
 * METADATA and INDEX, because we don't currently have a use case for decoding the VALUE or HASH_AIDX tables in a
 * generic fashion.
 */
public class GenericStreamStoreRowDecoder {
    private final StreamStoreCleanupMetadata cleanupMetadata;

    public GenericStreamStoreRowDecoder(StreamStoreCleanupMetadata cleanupMetadata) {
        this.cleanupMetadata = cleanupMetadata;
    }

    public GenericStreamIdentifier decodeIndexOrMetadataTableRow(byte[] rowComponent) {
        byte[] encodedStreamId = cleanupMetadata.numHashedRowComponents() == 0
                ? rowComponent
                : Arrays.copyOfRange(
                        rowComponent, StreamStoreHashEncodingUtils.getHashComponentBytes(), rowComponent.length);
        return ImmutableGenericStreamIdentifier.of(cleanupMetadata.streamIdType(), encodedStreamId);
    }
}
