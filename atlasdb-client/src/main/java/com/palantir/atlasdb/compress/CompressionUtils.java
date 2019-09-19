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

import com.palantir.atlasdb.table.description.ColumnValueDescription.Compression;
import com.palantir.common.base.Throwables;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.io.IOException;
import org.xerial.snappy.Snappy;

public final class CompressionUtils {
    private CompressionUtils() {
        // empty
    }

    public static byte[] compress(byte[] bytes, Compression compressionType) {
        if (compressionType == Compression.SNAPPY) {
            return compressWithSnappy(bytes);
        } else if (compressionType == Compression.NONE) {
            return bytes;
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public static byte[] decompress(byte[] bytes, Compression compressionType) {
        if (compressionType == Compression.SNAPPY) {
            return decompressWithSnappy(bytes);
        } else if (compressionType == Compression.NONE) {
            return bytes;
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public static byte[] compressWithSnappy(byte[] bytes) {
        try {
            return Snappy.compress(bytes);
        } catch (IOException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    public static byte[] decompressWithSnappy(byte[] bytes) {
        try {
            if (!Snappy.isValidCompressedBuffer(bytes)) {
                throw new SafeIllegalArgumentException("Cannot decompress these bytes using Snappy");
            }
            return Snappy.uncompress(bytes);
        } catch (IOException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }
}
