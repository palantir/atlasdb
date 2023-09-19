/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.hash.Hashing;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import org.assertj.core.util.Hexadecimals;
import org.junit.Test;

public class PtBytesTest {

    /**
     * 17.0.8.1
     * CE4F0293C7D7AD99116D792D6E616D652D69732D73657269657380000000497D26D8
     * 5642731691854769561
     * 1232938712
     */
    @Test
    public void testMe() {
        assertThat(System.getProperty("java.version")).contains("21");
        String series = "my-name-is-series";
        long myOffset = 1232938712;

        String expectedHash = "CE4F0293C7D7AD99116D792D6E616D652D69732D73657269657380000000497D26D8";

        long hash = computeHashFirstComponents(series);
        byte[] persistedBytes = persistToBytes(hash, series, myOffset);

        assertThat(Hexadecimals.toHexString(persistedBytes)).isEqualTo(expectedHash);
        hydrateFromBytes(persistedBytes);
        hydrateFromBytes(hexStringToByteArray(expectedHash));

        for (long offset = 0; offset < 100_000_000; offset++) {
            long newHash = computeHashFirstComponents("snapshot-updates-v1");
            byte[] newPersistedBytes = persistToBytes(newHash, "snapshot-updates-v1", myOffset);
            hydrateFromBytes(newPersistedBytes);
        }
        // assertThat(persistedBytes).isEqualTo(hexStringToByteArray(expectedHash));
    }

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    public static long computeHashFirstComponents(String series) {
        byte[] seriesBytes = EncodingUtils.encodeVarString(series);
        return Hashing.murmur3_128().hashBytes(EncodingUtils.add(seriesBytes)).asLong();
    }

    public static byte[] persistToBytes(long hashOfRowComponents, String series, long offset) {
        byte[] hashOfRowComponentsBytes = PtBytes.toBytes(Long.MIN_VALUE ^ hashOfRowComponents);
        byte[] seriesBytes = EncodingUtils.encodeVarString(series);
        byte[] offsetBytes = PtBytes.toBytes(Long.MIN_VALUE ^ offset);
        return EncodingUtils.add(hashOfRowComponentsBytes, seriesBytes, offsetBytes);
    }

    public static void hydrateFromBytes(byte[] __input) {
        int __index = 0;
        Long _hashOfRowComponents = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
        __index += 8;
        String series = EncodingUtils.decodeVarString(__input, __index);
        __index += EncodingUtils.sizeOfVarString(series);
        Long _offset = Long.MIN_VALUE ^ PtBytes.toLong(__input, __index);
        __index += 8;
    }
}
