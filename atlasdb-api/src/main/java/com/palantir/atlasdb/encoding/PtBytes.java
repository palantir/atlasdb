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
package com.palantir.atlasdb.encoding;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Ordering;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;

public final class PtBytes {
    private PtBytes() {}

    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    public static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;
    public static final Ordering<byte[]> BYTES_COMPARATOR = Ordering.from(UnsignedBytes.lexicographicalComparator());

    private static final Cache<String, byte[]> cache =
            CacheBuilder.newBuilder().maximumSize(5000).build();

    /**
     * Converts a byte array to a long value. Reverses {@link #toBytes(long)}
     */
    public static long toLong(byte[] bytes) {
        return Longs.fromByteArray(bytes);
    }

    /**
     * Converts a byte array to a long value. Assumes there will be {@link #SIZEOF_LONG} bytes
     * available.
     */
    public static long toLong(byte[] bytes, int offset) {
        return Longs.fromBytes(
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
                bytes[offset + 4],
                bytes[offset + 5],
                bytes[offset + 6],
                bytes[offset + 7]);
    }

    /**
     * Convert a long value to a byte array using big-endian.
     */
    public static byte[] toBytes(long val) {
        return Longs.toByteArray(val);
    }

    /**
     * Converts a string to a UTF-8 byte array.
     */
    public static byte[] toBytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Converts a string to a UTF-8 byte array. This method will cache results and return cached
     * results, so the resulting byte arrays must not be modified.
     */
    public static byte[] toCachedBytes(String str) {
        byte[] bytes = cache.getIfPresent(str);
        if (bytes == null) {
            bytes = toBytes(str);
            cache.put(str, bytes);
        }
        return bytes;
    }

    public static byte[] tail(final byte[] arr, final int length) {
        if (arr.length < length) {
            return null;
        }
        byte[] result = new byte[length];
        System.arraycopy(arr, arr.length - length, result, 0, length);
        return result;
    }

    public static byte[] head(final byte[] arr, final int length) {
        if (arr.length < length) {
            return null;
        }
        byte[] result = new byte[length];
        System.arraycopy(arr, 0, result, 0, length);
        return result;
    }

    public static String toString(final byte[] arr) {
        if (arr == null) {
            return null;
        }
        return toString(arr, 0, arr.length);
    }

    public static String toString(final byte[] arr, int off, int len) {
        if (arr == null) {
            return null;
        }
        if (len == 0) {
            return "";
        }
        return new String(arr, off, len, StandardCharsets.UTF_8);
    }

    public static String encodeHexString(@Nullable byte[] name) {
        if (name == null) {
            return "";
        }
        return BaseEncoding.base16().lowerCase().encode(name);
    }

    public static byte[] decodeHexString(@Nullable String hexString) {
        if (hexString == null) {
            return PtBytes.EMPTY_BYTE_ARRAY;
        }
        return BaseEncoding.base16().lowerCase().decode(hexString.toLowerCase());
    }

    public static final Function<byte[], String> BYTES_TO_HEX_STRING = PtBytes::encodeHexString;

    public static void addIfNotEmpty(MoreObjects.ToStringHelper helper, String name, byte[] bytes) {
        if (bytes != null && bytes.length > 0) {
            helper.add(name, encodeHexString(bytes));
        }
    }

    public static String encodeBase64String(byte[] data) {
        return BaseEncoding.base64().encode(data);
    }

    public static String encodeBase64String(byte[] data, int offset, int length) {
        return BaseEncoding.base64().encode(data, offset, length);
    }

    public static byte[] decodeBase64(String data) {
        return BaseEncoding.base64().decode(data);
    }

    /**
     * Return true if the byte array on the right is a prefix of the byte array on the left.
     */
    public static boolean startsWith(byte[] bytes, byte[] prefix) {
        if (bytes == null || prefix == null || bytes.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (bytes[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Compare two byte arrays.
     *
     * @return 0 if equal, &lt; 0 if left is less than right, etc
     */
    public static int compareTo(final byte[] left, final byte[] right) {
        return UnsignedBytes.lexicographicalComparator().compare(left, right);
    }
}
