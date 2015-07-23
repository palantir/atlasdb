// Copyright 2015 Palantir Technologies
//
// Licensed under the BSD-3 License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://opensource.org/licenses/BSD-3-Clause
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.encoding;

import com.google.common.base.Charsets;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Ordering;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;

public final class PtBytes {
    private PtBytes() { }

    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    public static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;
    public static Ordering<byte[]> BYTES_COMPARATOR = Ordering.from(UnsignedBytes.lexicographicalComparator());

    private static final Cache<String, byte[]> cache = CacheBuilder.newBuilder().maximumSize(5000).build();

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
    public static byte[] toBytes(String s) {
        return s.getBytes(Charsets.UTF_8);
    }

    /**
     * Converts a string to a UTF-8 byte array. This method will cache results and return cached
     * results, so the resulting byte arrays must not be modified.
     */
    public static byte[] toCachedBytes(String s) {
        byte[] bytes = cache.getIfPresent(s);
        if (bytes == null) {
            bytes = toBytes(s);
            cache.put(s, bytes);
        }
        return bytes;
    }

    public static byte[] tail(final byte[] a, final int length) {
        if (a.length < length) {
            return null;
        }
        byte[] result = new byte[length];
        System.arraycopy(a, a.length - length, result, 0, length);
        return result;
    }

    public static byte[] head(final byte[] a, final int length) {
        if (a.length < length) {
            return null;
        }
        byte[] result = new byte[length];
        System.arraycopy(a, 0, result, 0, length);
        return result;
    }

    public static String toString(final byte[] b) {
        if (b == null) {
            return null;
        }
        return toString(b, 0, b.length);
    }

    public static String toString(final byte[] b, int off, int len) {
        if (b == null) {
            return null;
        }
        if (len == 0) {
            return "";
        }
        return new String(b, off, len, Charsets.UTF_8);
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
     * @return 0 if equal, < 0 if left is less than right, etc.
     */
    public static int compareTo(final byte[] left, final byte[] right) {
        return UnsignedBytes.lexicographicalComparator().compare(left, right);
    }
}
