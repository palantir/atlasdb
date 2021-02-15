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
package com.palantir.atlasdb.ptobject;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import com.google.protobuf.CodedOutputStream;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ValueByteOrder;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.common.annotation.Output;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.ArrayUtils;

@SuppressWarnings("checkstyle:all") // too many warnings to fix
public final class EncodingUtils {
    private EncodingUtils() {
        // empty
    }

    public static byte[] encodeVarLongAndFixedLong(long realmId, long v2) {
        byte[] b1 = encodeVarLong(realmId);
        byte[] b2 = PtBytes.toBytes(v2);
        return Bytes.concat(b1, b2);
    }

    public static long decodeFixedLongAfterVarLong(long realmId, byte[] k) {
        int size = sizeOfVarLong(realmId);
        Preconditions.checkArgument(k.length == size + PtBytes.SIZEOF_LONG);
        return PtBytes.toLong(k, size);
    }

    /**
     * Size of the encoded value will be the same as the protobuf encoding.
     * However, we have to use our own encoding and not the proto one because we need the bytes
     * to be lexicographically ordered so we can do range scans correctly.
     */
    public static byte[] encodeVarLong(long value) {
        int size = sizeOfVarLong(value);
        byte[] ret = new byte[size];
        encodeVarLongForSize(value, ret, size);
        return ret;
    }

    public static byte[] encodeUnsignedVarLong(long value) {
        return encodeVarLong(checkUnsigned(value));
    }

    public static byte[] encodeSignedVarLong(long value) {
        final boolean negative = value < 0;
        int size = sizeOfSignedVarLong(value);
        value ^= (value >> 63);
        byte[] ret = new byte[size];
        encodeVarLongForSize(value, ret, size + 1);
        if (negative) {
            flipAllBitsInPlace(ret);
        }
        return ret;
    }

    /**
     * There will be size-1 bits set before there is a zero.
     * All the bits of value will or-ed (|=) onto the the passed byte[].
     * @param size must be <= 17 (but will most likely be 10 or 11 at most)
     */
    private static void encodeVarLongForSize(long value, @Output byte[] ret, int size) {
        int end = 0;
        if (size > 8) {
            ret[0] = (byte) 0xff;
            end = 1;
            size -= 8;
        }
        ret[end] = (byte) ((0xff << (9 - size)) & 0xff);

        int index = ret.length;
        while (index-- > end) {
            ret[index] |= (byte) ((int) value & 0xff);
            value >>>= 8;
        }
    }

    public static int sizeOfVarLong(long value) {
        return CodedOutputStream.computeRawVarint64Size(value);
    }

    public static int sizeOfUnsignedVarLong(long value) {
        return sizeOfVarLong(checkUnsigned(value));
    }

    public static int sizeOfSignedVarLong(long value) {
        value ^= (value >> 63);
        value <<= 1;
        return sizeOfVarLong(value);
    }

    public static long decodeVarLong(byte[] encoded) {
        return decodeVarLong(encoded, 0);
    }

    public static long decodeVarLong(byte[] encoded, int offset) {
        return decodeVarLong(encoded, offset, 0);
    }

    private static long decodeFlippedVarLong(byte[] encoded, int offset) {
        return decodeVarLong(encoded, offset, -1);
    }

    private static long decodeVarLong(byte[] encoded, int offset, int flipByte) {
        int first = encoded[offset] ^ flipByte;
        if (first >= 0) {
            return first;
        }
        int bitsBeforeZero = Integer.numberOfLeadingZeros(~first) - 24;
        if (bitsBeforeZero == 8 && (encoded[offset + 1] ^ flipByte) < 0) {
            bitsBeforeZero++;
            if (((encoded[offset + 1] ^ flipByte) & 0x40) != 0) {
                throw new SafeIllegalArgumentException("bad varlong, too big");
            }
        }
        int size = bitsBeforeZero + 1;

        int index = size / 8;
        int mask = size % 8;
        long ret = 0;
        while (index < size) {
            int b = ((encoded[offset + index] ^ flipByte) & (0xff >>> mask));
            ret <<= 8;
            ret |= b;
            mask = 0;
            index++;
        }
        return ret;
    }

    public static long decodeUnsignedVarLong(byte[] encoded) {
        return checkUnsigned(decodeVarLong(encoded));
    }

    public static long decodeSignedVarLong(byte[] encoded) {
        return decodeSignedVarLong(encoded, 0);
    }

    public static long decodeUnsignedVarLong(byte[] encoded, int offset) {
        return checkUnsigned(decodeVarLong(encoded, offset));
    }

    public static long decodeFlippedUnsignedVarLong(byte[] encoded, int offset) {
        return checkUnsigned(decodeFlippedVarLong(encoded, offset));
    }

    public static long decodeSignedVarLong(byte[] encoded, int offset) {
        return decodeSignedVarLong(encoded, offset, 0);
    }

    public static long decodeFlippedSignedVarLong(byte[] encoded, int offset) {
        return decodeSignedVarLong(encoded, offset, -1);
    }

    private static long decodeSignedVarLong(byte[] encoded, int offset, int flipByte) {
        boolean isNegative = ((encoded[offset] ^ flipByte) & 0x80) == 0;
        if (isNegative) {
            flipByte ^= -1;
        }
        int first = encoded[offset] ^ flipByte;
        int bitsBeforeZero = Integer.numberOfLeadingZeros(~first) - 24;
        if (bitsBeforeZero == 8 && (encoded[offset + 1] ^ flipByte) < 0) {
            bitsBeforeZero++;
            if (((encoded[offset + 1] ^ flipByte) & 0x40) != 0) {
                bitsBeforeZero++;
                if (((encoded[offset + 1] ^ flipByte) & 0x20) != 0) {
                    throw new SafeIllegalArgumentException("bad varlong, too big");
                }
            }
        }
        int size = bitsBeforeZero;
        int index = size / 8;
        int mask = size % 8;
        long ret = 0;
        while (index < size) {
            int b = ((encoded[offset + index] ^ flipByte) & (0xff >>> mask));
            ret <<= 8;
            ret |= b;
            mask = 0;
            index++;
        }
        if (isNegative) {
            return ret ^ -1L;
        }
        return ret;
    }

    public static String decodeVarString(byte[] bytes) {
        return decodeVarString(bytes, 0);
    }

    public static String decodeVarString(byte[] bytes, int offset) {
        int len = (int) decodeVarLong(bytes, offset);
        offset += sizeOfVarLong(len);
        return PtBytes.toString(bytes, offset, len);
    }

    public static byte[] decodeSizedBytes(byte[] bytes, int offset) {
        int len = (int) decodeVarLong(bytes, offset);
        offset += sizeOfVarLong(len);
        return Arrays.copyOfRange(bytes, offset, offset + len);
    }

    /**
     * Warning: This function mutates the input array by flipping the bytes
     * that the string is parsed from (but not the bytes that the length of
     * the string is parsed from).
     */
    public static String decodeFlippedVarString(byte[] bytes, int offset) {
        int len = (int) decodeFlippedVarLong(bytes, offset);
        offset += sizeOfVarLong(len);
        return PtBytes.toString(flipAllBitsInPlace(bytes, offset, len), offset, len);
    }

    public static byte[] decodeFlippedSizedBytes(byte[] bytes, int offset) {
        int len = (int) decodeFlippedVarLong(bytes, offset);
        offset += sizeOfVarLong(len);
        return Arrays.copyOfRange(flipAllBitsInPlace(bytes, offset, len), offset, offset + len);
    }

    public static int sizeOfVarString(String str) {
        return sizeOfSizedBytes(PtBytes.toBytes(str));
    }

    public static int sizeOfSizedBytes(byte[] bytes) {
        return bytes.length + sizeOfVarLong(bytes.length);
    }

    public static byte[] encodeVarString(String strValue) {
        return encodeSizedBytes(PtBytes.toBytes(strValue));
    }

    public static byte[] encodeSizedBytes(byte[] bytes) {
        byte[] len = encodeVarLong(bytes.length);
        return Bytes.concat(len, bytes);
    }

    public static byte[] encodeUUID(UUID uuid) {
        return ByteBuffer.allocate(2 * Longs.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }

    public static UUID decodeUUID(byte[] bytes, int offset) {
        ByteBuffer buf = ByteBuffer.wrap(bytes, offset, 2 * Longs.BYTES).order(ByteOrder.BIG_ENDIAN);
        long mostSigBits = buf.getLong();
        long leastSigBits = buf.getLong();
        return new UUID(mostSigBits, leastSigBits);
    }

    public static UUID decodeFlippedUUID(byte[] bytes, int offset) {
        ByteBuffer buf = ByteBuffer.wrap(bytes, offset, 2 * Longs.BYTES).order(ByteOrder.BIG_ENDIAN);
        long mostSigBits = -1L ^ buf.getLong();
        long leastSigBits = -1L ^ buf.getLong();
        return new UUID(mostSigBits, leastSigBits);
    }

    public static byte[] flipAllBits(byte[] bytes) {
        return flipAllBits(bytes, 0);
    }

    /**
     * This flips the bits starting at index and returns a new byte[].
     * @return byte[] that is bytes.length - index long
     */
    public static byte[] flipAllBits(byte[] bytes, int index) {
        byte[] ret = new byte[bytes.length - index];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = (byte) (bytes[index + i] ^ 0xff);
        }
        return ret;
    }

    /**
     * This flips the bits and returns the same byte[].
     * @return bytes
     */
    public static byte[] flipAllBitsInPlace(byte[] bytes) {
        return flipAllBitsInPlace(bytes, 0, bytes.length);
    }

    /**
     * This flips the bits starting at index and returns the same byte[].
     * @return bytes
     */
    public static byte[] flipAllBitsInPlace(byte[] bytes, int index) {
        return flipAllBitsInPlace(bytes, index, bytes.length - index);
    }

    /**
     * This flips the bits in the range [index, index + length) and returns the same byte[].
     * @return bytes
     */
    public static byte[] flipAllBitsInPlace(byte[] bytes, int index, int length) {
        int endIndex = Math.min(bytes.length, index + length);
        for (int i = index; i < endIndex; i++) {
            bytes[i] = (byte) (bytes[i] ^ 0xff);
        }
        return bytes;
    }

    public static byte[] add(byte[] b1) {
        return b1;
    }

    public static byte[] add(byte[] b1, byte[] b2) {
        return Bytes.concat(b1, b2);
    }

    public static byte[] add(byte[] b1, byte[] b2, byte[] b3) {
        return Bytes.concat(b1, b2, b3);
    }

    public static byte[] add(byte[]... bytes) {
        int length = 0;
        for (byte[] b : bytes) {
            length += b.length;
        }

        byte[] result = new byte[length];
        int i = 0;
        for (byte[] b : bytes) {
            System.arraycopy(b, 0, result, i, b.length);
            i += b.length;
        }
        return result;
    }

    public static byte[] getBytesFromOffsetToEnd(byte[] b1, int offset) {
        if (offset == 0) {
            return b1;
        }
        return PtBytes.tail(b1, b1.length - offset);
    }

    public static byte[] get32Bytes(byte[] b1, int offset) {
        return ArrayUtils.subarray(b1, offset, offset + 32);
    }

    public static long decodeLittleEndian(byte[] value, int offset) {
        byte[] subArray = ArrayUtils.subarray(value, offset, offset + PtBytes.SIZEOF_LONG);
        ArrayUtils.reverse(subArray);
        return PtBytes.toLong(subArray);
    }

    public static byte[] encodeLittleEndian(long val) {
        byte[] bytes = PtBytes.toBytes(val);
        ArrayUtils.reverse(bytes);
        return bytes;
    }

    private static long checkUnsigned(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("Expected unsigned value: " + value);
        }
        return value;
    }

    public static class EncodingType {
        private final ValueType type;
        private final ValueByteOrder order;

        public EncodingType(ValueType type) {
            this(type, ValueByteOrder.ASCENDING);
        }

        public EncodingType(ValueType type, ValueByteOrder order) {
            this.type = type;
            this.order = order;
        }

        public ValueType getType() {
            return type;
        }

        public ValueByteOrder getOrder() {
            return order;
        }
    }

    public static byte[] toBytes(List<EncodingType> types, List<Object> components) {
        Preconditions.checkArgument(types.size() == components.size());

        byte[][] bytes = new byte[types.size()][];
        for (int i = 0; i < types.size(); i++) {
            EncodingType encodingType = types.get(i);
            Object obj = components.get(i);
            bytes[i] = encodingType.getType().convertFromJava(obj);
            if (encodingType.getOrder() == ValueByteOrder.DESCENDING) {
                EncodingUtils.flipAllBitsInPlace(bytes[i]);
            }
        }
        return EncodingUtils.add(bytes);
    }

    public static List<Object> fromBytes(byte[] b, List<EncodingType> types) {
        List<Object> result = new ArrayList<>();
        int index = 0;
        boolean lastDesc = false;

        for (int i = 0; i < types.size(); i++) {
            EncodingType encodingType = types.get(i);
            if (lastDesc ^ encodingType.getOrder() == ValueByteOrder.DESCENDING) {
                b = EncodingUtils.flipAllBits(b, index);
                index = 0;
                lastDesc = !lastDesc;
            }
            Object value = encodingType.getType().convertToJava(b, index);
            result.add(value);
            index += encodingType.getType().sizeOf(value);
        }

        return result;
    }

    public static Long decodeNullableFixedLong(byte[] value, int offset) {
        if (value[offset] == 0) {
            return null;
        } else {
            return Long.MIN_VALUE ^ PtBytes.toLong(value, offset + 1);
        }
    }

    public static Long decodeFlippedNullableFixedLong(byte[] value, int offset) {
        if (value[offset] == -1) {
            return null;
        } else {
            return Long.MAX_VALUE ^ PtBytes.toLong(value, offset + 1);
        }
    }

    public static byte[] encodeNullableFixedLong(Long value) {
        if (value == null) {
            return new byte[9];
        } else {
            return ArrayUtils.addAll(new byte[] {1}, PtBytes.toBytes(Long.MIN_VALUE ^ value));
        }
    }
}
