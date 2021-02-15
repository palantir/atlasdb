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
package com.palantir.atlasdb.compress;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.table.description.ColumnValueDescription.Compression;
import java.util.Arrays;
import org.junit.Test;

public class CompressionUtilsTest {
    @Test
    public void testCompressAndDecompress() {
        byte[] original = new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};

        byte[] compressed = CompressionUtils.compress(original, Compression.NONE);
        assertThat(compressed).isEqualTo(original);
        byte[] decompressed = CompressionUtils.decompress(compressed, Compression.NONE);
        assertThat(decompressed).isEqualTo(original);

        compressed = CompressionUtils.compress(original, Compression.SNAPPY);
        assertThat(Arrays.equals(original, compressed)).isFalse();
        decompressed = CompressionUtils.decompress(compressed, Compression.SNAPPY);
        assertThat(decompressed).isEqualTo(original);
    }

    @Test
    public void testCompressAndDecompressWithSnappy() {
        byte[] original = new byte[1024];
        byte[] compressed = CompressionUtils.compressWithSnappy(original);
        assertThat(Arrays.equals(original, compressed)).isFalse();
        byte[] decompressed = CompressionUtils.decompressWithSnappy(compressed);
        assertThat(decompressed).isEqualTo(original);
    }

    @Test
    public void testDecompressException() {
        byte[] compressed = new byte[] {1, 2, 3}; // invalid
        boolean threwIllegalArgumentException = false;
        try {
            CompressionUtils.decompress(compressed, Compression.SNAPPY);
        } catch (IllegalArgumentException e) {
            threwIllegalArgumentException = true;
        }
        assertThat(threwIllegalArgumentException).isTrue();
    }

    @Test
    public void testDecompressExceptionWithSnappy() {
        byte[] compressed = new byte[] {1, 2, 3}; // invalid
        boolean threwIllegalArgumentException = false;
        try {
            CompressionUtils.decompressWithSnappy(compressed);
        } catch (IllegalArgumentException e) {
            threwIllegalArgumentException = true;
        }
        assertThat(threwIllegalArgumentException).isTrue();
    }
}
