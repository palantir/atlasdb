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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.StreamPersistence;
import com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata;
import com.palantir.atlasdb.schema.cleanup.ImmutableStreamStoreCleanupMetadata;
import com.palantir.atlasdb.schema.stream.StreamTableType;
import com.palantir.atlasdb.table.description.ValueType;

public class StreamStoreMetadataReaderTest {
    private static final StreamMetadata STREAM_METADATA_1 = StreamMetadata.newBuilder()
            .setLength(123L)
            .setHash(ByteString.copyFromUtf8("abcdefgh"))
            .setStatus(StreamPersistence.Status.STORED)
            .build();
    private static final StreamMetadata STREAM_METADATA_2 = StreamMetadata.newBuilder()
            .setLength(321L)
            .setHash(ByteString.copyFromUtf8("qwertyuiop"))
            .setStatus(StreamPersistence.Status.STORING)
            .build();

    private static final byte[] RAW_STREAM_METADATA_1 = STREAM_METADATA_1.toByteArray();
    private static final byte[] RAW_STREAM_METADATA_2 = STREAM_METADATA_2.toByteArray();
    private static final byte[] ARBITRARY_BYTE = { 43 };

    private static final Cell CELL_1 = Cell.create(PtBytes.toBytes("abc"), PtBytes.toBytes("def"));
    private static final Cell CELL_2 = Cell.create(PtBytes.toBytes("foo"), PtBytes.toBytes("bar"));

    private final StreamStoreMetadataReader reader = new StreamStoreMetadataReader(
            TableReference.create(Namespace.DEFAULT_NAMESPACE, StreamTableType.METADATA.getTableName("abc")),
            new GenericStreamStoreCellCreator(
                    ImmutableStreamStoreCleanupMetadata.builder()
                            .numHashedRowComponents(1)
                            .streamIdType(ValueType.VAR_LONG)
                            .build()));

    @Test
    public void canParseStreamMetadata() {
        Map<Cell, byte[]> rawStreamMetadataMap =
                ImmutableMap.of(CELL_1, RAW_STREAM_METADATA_1, CELL_2, RAW_STREAM_METADATA_2);


        assertThat(reader.parseStreamMetadata(rawStreamMetadataMap))
                .isEqualTo(ImmutableMap.of(CELL_1, STREAM_METADATA_1, CELL_2, STREAM_METADATA_2));
    }

    @Test
    public void resilientToUnparseableStreamMetadata() {
        Map<Cell, byte[]> rawStreamMetadataMap =
                ImmutableMap.of(CELL_1, RAW_STREAM_METADATA_1, CELL_2, ARBITRARY_BYTE);

        assertThat(reader.parseStreamMetadata(rawStreamMetadataMap))
                .isEqualTo(ImmutableMap.of(CELL_1, STREAM_METADATA_1));
    }
}
