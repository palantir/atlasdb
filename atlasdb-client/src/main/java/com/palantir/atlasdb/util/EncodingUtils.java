/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.util;

import javax.annotation.Nullable;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.table.description.TableMetadata;

public class EncodingUtils {
    private EncodingUtils() {
        // utility
    }

    public static String hexToString(@Nullable String hexString) {
        hexString = standardize(hexString);
        return PtBytes.toString(PtBytes.decodeHexString(hexString));
    }

    public static String stringToHex(@Nullable String string) {
        return "0x" + PtBytes.encodeHexString(PtBytes.toBytes(string));
    }

    public static TableMetadata hexToMetadata(String hexString) {
        hexString = standardize(hexString);
        return TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(PtBytes.decodeHexString(hexString));
    }

    public static String metadataToHex(TableMetadata metadata) {
        return "0x" + PtBytes.encodeHexString(metadata.persistToBytes());
    }

    public static long transformTimestamp(long ts) {
        return ts ^ -1;
    }

    private static String standardize(@Nullable String hexString) {
        hexString = hexString.toLowerCase();
        if (hexString.startsWith("0x")) {
            hexString = hexString.substring(2);
        }
        return hexString;
    }

    public static void main(String[] args) {
        // Utility main method for convenience
    }
}
