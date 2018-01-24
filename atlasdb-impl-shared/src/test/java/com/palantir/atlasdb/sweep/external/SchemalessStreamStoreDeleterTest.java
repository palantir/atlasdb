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

import org.junit.Test;

import com.google.protobuf.ByteString;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.protos.generated.StreamPersistence;
import com.palantir.atlasdb.stream.GenericStreamStore;

public class SchemalessStreamStoreDeleterTest {
    private static final ByteString BYTE_STRING = ByteString.copyFrom(new byte[] { 1, 2, 3 });
    private static final Namespace NAMESPACE = Namespace.DEFAULT_NAMESPACE;
    private static final String SHORT_NAME = "s";

    private static final int BLOCK_SIZE = GenericStreamStore.BLOCK_SIZE_IN_BYTES;

    private final SchemalessStreamStoreDeleter deleter = new SchemalessStreamStoreDeleter(
            NAMESPACE,
            SHORT_NAME,
            null);

    @Test
    public void getNumberOfBlocksFromMetadata_Normal() {
        assertThat(getNumberOfBlocksFromDeleter(3 * BLOCK_SIZE + BLOCK_SIZE / 2)).isEqualTo(4);
        assertThat(getNumberOfBlocksFromDeleter(1)).isEqualTo(1);
    }

    @Test
    public void getNumberOfBlocksFromMetadata_ExactMatch() {
        assertThat(getNumberOfBlocksFromDeleter(10 * BLOCK_SIZE)).isEqualTo(10);
        assertThat(getNumberOfBlocksFromDeleter(744 * BLOCK_SIZE)).isEqualTo(744);
    }

    @Test
    public void getNumberOfBlocksFromMetadata_OffByOnes() {
        assertThat(getNumberOfBlocksFromDeleter(10 * BLOCK_SIZE - 1)).isEqualTo(10);
        assertThat(getNumberOfBlocksFromDeleter(10 * BLOCK_SIZE + 1)).isEqualTo(11);
    }

    @Test
    public void getNumberOfBlocksFromMetadata_Zero() {
        assertThat(getNumberOfBlocksFromDeleter(0)).isEqualTo(0);
    }

    private long getNumberOfBlocksFromDeleter(int length) {
        return deleter.getNumberOfBlocksFromMetadata(createStreamMetadataWithLength(length));
    }

    private static StreamPersistence.StreamMetadata createStreamMetadataWithLength(int length) {
        return StreamPersistence.StreamMetadata.newBuilder()
                .setHash(BYTE_STRING)
                .setLength(length)
                .setStatus(StreamPersistence.Status.STORED)
                .build();
    }
}
