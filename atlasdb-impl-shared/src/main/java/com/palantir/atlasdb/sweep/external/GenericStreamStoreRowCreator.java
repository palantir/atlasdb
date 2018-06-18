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

import com.google.protobuf.ByteString;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.schema.cleanup.StreamStoreCleanupMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.util.crypto.Sha256Hash;

public class GenericStreamStoreRowCreator {
    private final StreamStoreCleanupMetadata cleanupMetadata;

    public GenericStreamStoreRowCreator(StreamStoreCleanupMetadata cleanupMetadata) {
        this.cleanupMetadata = cleanupMetadata;
    }

    public byte[] constructValueTableRow(GenericStreamIdentifier streamId, long blockId) {
        byte[] hashComponent = StreamStoreHashEncodingUtils.getHashComponent(
                cleanupMetadata.numHashedRowComponents(), streamId, blockId);
        byte[] streamIdComponent = streamId.data();
        byte[] blockIdComponent = ValueType.VAR_LONG.convertFromJava(blockId);
        return EncodingUtils.add(hashComponent, streamIdComponent, blockIdComponent);
    }

    public byte[] constructIndexOrMetadataTableRow(GenericStreamIdentifier streamId) {
        byte[] hashComponent = StreamStoreHashEncodingUtils.getHashComponentWithoutBlockId(
                cleanupMetadata.numHashedRowComponents(), streamId);
        byte[] streamIdComponent = streamId.data();
        return EncodingUtils.add(hashComponent, streamIdComponent);
    }

    public byte[] constructHashTableRow(ByteString streamHash) {
        Sha256Hash hash = Sha256Hash.EMPTY;
        if (streamHash != ByteString.EMPTY) {
            hash = new Sha256Hash(streamHash.toByteArray());
        }
        return hash.getBytes();
    }
}
