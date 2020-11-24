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
package com.palantir.atlasdb.table.description;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.util.Pair;
import com.palantir.util.crypto.Sha256Hash;
import java.io.IOException;

public enum ValueType {
    /**
     * Value type for variable-encoded non-negative longs. Numbers closer to
     * zero will require less space.
     *
     * Use {@link #VAR_SIGNED_LONG} instead if you need negative numbers.
     *
     * This value type supports range scans.
     */
    VAR_LONG {
        @Override
        public int getMaxValueSize() {
            return 10;
        }

        @Override
        public Long convertToJava(byte[] value, int offset) {
            return EncodingUtils.decodeUnsignedVarLong(value, offset);
        }

        @Override
        public Pair<String, Integer> convertToString(byte[] value, int offset) {
            long val = EncodingUtils.decodeVarLong(value, offset);
            return Pair.create(String.valueOf(val), EncodingUtils.sizeOfVarLong(val));
        }

        @Override
        public Pair<String, Integer> convertToJson(byte[] value, int offset) {
            return convertToString(value, offset);
        }

        @Override
        public byte[] convertFromString(String strValue) {
            return EncodingUtils.encodeVarLong(Long.parseLong(strValue));
        }

        @Override
        public byte[] convertFromJson(String jsonValue) {
            return convertFromString(jsonValue);
        }

        @Override
        public Class getJavaClass() {
            return long.class;
        }

        @Override
        public Class getJavaObjectClass() {
            return Long.class;
        }

        @Override
        public String getHydrateCode(String inputName, String indexName) {
            return "EncodingUtils.decodeUnsignedVarLong(" + inputName + ", " + indexName + ")";
        }

        @Override
        public String getFlippedHydrateCode(String inputName, String indexName) {
            return "EncodingUtils.decodeFlippedUnsignedVarLong(" + inputName + ", " + indexName + ")";
        }

        @Override
        public String getHydrateSizeCode(String variableName) {
            return "EncodingUtils.sizeOfUnsignedVarLong(" + variableName + ")";
        }

        @Override
        public byte[] convertFromJava(Object value) {
            com.palantir.logsafe.Preconditions.checkArgument(value instanceof Long);
            return EncodingUtils.encodeUnsignedVarLong((Long) value);
        }

        @Override
        public int sizeOf(Object value) {
            com.palantir.logsafe.Preconditions.checkArgument(value instanceof Long);
            return EncodingUtils.sizeOfUnsignedVarLong((Long) value);
        }

        @Override
        public String getPersistCode(String variableName) {
            return "EncodingUtils.encodeUnsignedVarLong(" + variableName + ")";
        }
    },
    /**
     * This value type supports range scans. Neighboring number will be written next to each other.
     * This efficiently encodes negative numbers as well as positive numbers.  The closer the number
     * is to zero, the more efficiently it is encoded.  This also does correct sorting of negative
     * numbers, so they occur before positive numbers.
     */
    VAR_SIGNED_LONG {
        @Override
        public int getMaxValueSize() {
            return 10;
        }

        @Override
        public Long convertToJava(byte[] value, int offset) {
            return EncodingUtils.decodeSignedVarLong(value, offset);
        }

        @Override
        public Pair<String, Integer> convertToString(byte[] value, int offset) {
            long val = EncodingUtils.decodeSignedVarLong(value, offset);
            return Pair.create(String.valueOf(val), EncodingUtils.sizeOfSignedVarLong(val));
        }

        @Override
        public Pair<String, Integer> convertToJson(byte[] value, int offset) {
            return convertToString(value, offset);
        }

        @Override
        public byte[] convertFromString(String strValue) {
            return EncodingUtils.encodeSignedVarLong(Long.parseLong(strValue));
        }

        @Override
        public byte[] convertFromJson(String jsonValue) {
            return convertFromString(jsonValue);
        }

        @Override
        public Class getJavaClass() {
            return long.class;
        }

        @Override
        public Class getJavaObjectClass() {
            return Long.class;
        }

        @Override
        public String getHydrateCode(String inputName, String indexName) {
            return "EncodingUtils.decodeSignedVarLong(" + inputName + ", " + indexName + ")";
        }

        @Override
        public String getFlippedHydrateCode(String inputName, String indexName) {
            return "EncodingUtils.decodeFlippedSignedVarLong(" + inputName + ", " + indexName + ")";
        }

        @Override
        public String getHydrateSizeCode(String variableName) {
            return "EncodingUtils.sizeOfSignedVarLong(" + variableName + ")";
        }

        @Override
        public byte[] convertFromJava(Object value) {
            com.palantir.logsafe.Preconditions.checkArgument(value instanceof Long);
            return EncodingUtils.encodeSignedVarLong((Long) value);
        }

        @Override
        public int sizeOf(Object value) {
            com.palantir.logsafe.Preconditions.checkArgument(value instanceof Long);
            return EncodingUtils.sizeOfSignedVarLong((Long) value);
        }

        @Override
        public String getPersistCode(String variableName) {
            return "EncodingUtils.encodeSignedVarLong(" + variableName + ")";
        }
    },
    /**
     * This value type supports range scans.  Sequential numbers will be written next to each other.
     * This encoding is for fixed long values.  It is always 8 bytes.
     * It correctly sorts negative numbers before positive numbers in it's encoding.
     */
    FIXED_LONG {
        @Override
        public int getMaxValueSize() {
            return 8;
        }

        @Override
        public Long convertToJava(byte[] value, int offset) {
            return Long.MIN_VALUE ^ PtBytes.toLong(value, offset);
        }

        @Override
        public Pair<String, Integer> convertToString(byte[] value, int offset) {
            long val = Long.MIN_VALUE ^ PtBytes.toLong(value, offset);
            return Pair.create(String.valueOf(val), PtBytes.SIZEOF_LONG);
        }

        @Override
        public Pair<String, Integer> convertToJson(byte[] value, int offset) {
            return convertToString(value, offset);
        }

        @Override
        public byte[] convertFromString(String strValue) {
            return PtBytes.toBytes(Long.MIN_VALUE ^ Long.parseLong(strValue));
        }

        @Override
        public byte[] convertFromJson(String jsonValue) {
            return convertFromString(jsonValue);
        }

        @Override
        public Class getJavaClass() {
            return long.class;
        }

        @Override
        public Class getJavaObjectClass() {
            return Long.class;
        }

        @Override
        public String getHydrateCode(String inputName, String indexName) {
            return "Long.MIN_VALUE ^ PtBytes.toLong(" + inputName + ", " + indexName + ")";
        }

        @Override
        public String getFlippedHydrateCode(String inputName, String indexName) {
            return "Long.MAX_VALUE ^ PtBytes.toLong(" + inputName + ", " + indexName + ")";
        }

        @Override
        public String getHydrateSizeCode(String input) {
            return "8";
        }

        @Override
        public byte[] convertFromJava(Object value) {
            com.palantir.logsafe.Preconditions.checkArgument(value instanceof Long);
            return PtBytes.toBytes(Long.MIN_VALUE ^ (Long) value);
        }

        @Override
        public int sizeOf(Object value) {
            com.palantir.logsafe.Preconditions.checkArgument(value instanceof Long);
            return 8;
        }

        @Override
        public String getPersistCode(String variableName) {
            return "PtBytes.toBytes(Long.MIN_VALUE ^ " + variableName + ")";
        }
    },
    /**
     * This value type does NOT support range scans. This encoding is {@link PtBytes#toBytes(long)} but with
     * the bytes reversed. This type is good if you don't need to support range scans because keys next
     * to each other won't be written next to each other.  This is good because it will spread out
     * the load of writes to many different ranges.
     */
    FIXED_LONG_LITTLE_ENDIAN {
        @Override
        public int getMaxValueSize() {
            return 8;
        }

        @Override
        public Long convertToJava(byte[] value, int offset) {
            return EncodingUtils.decodeLittleEndian(value, offset);
        }

        @Override
        public Pair<String, Integer> convertToString(byte[] value, int offset) {
            long val = EncodingUtils.decodeLittleEndian(value, offset);
            return Pair.create(String.valueOf(val), PtBytes.SIZEOF_LONG);
        }

        @Override
        public Pair<String, Integer> convertToJson(byte[] value, int offset) {
            return convertToString(value, offset);
        }

        @Override
        public byte[] convertFromString(String strValue) {
            return EncodingUtils.encodeLittleEndian(Long.parseLong(strValue));
        }

        @Override
        public byte[] convertFromJson(String jsonValue) {
            return convertFromString(jsonValue);
        }

        @Override
        public Class getJavaClass() {
            return long.class;
        }

        @Override
        public Class getJavaObjectClass() {
            return Long.class;
        }

        @Override
        public String getHydrateCode(String inputName, String indexName) {
            return "EncodingUtils.decodeLittleEndian(" + inputName + ", " + indexName + ")";
        }

        @Override
        public String getFlippedHydrateCode(String inputName, String indexName) {
            return "-1L ^ EncodingUtils.decodeLittleEndian(" + inputName + ", " + indexName + ")";
        }

        @Override
        public String getHydrateSizeCode(String input) {
            return "8";
        }

        @Override
        public byte[] convertFromJava(Object value) {
            com.palantir.logsafe.Preconditions.checkArgument(value instanceof Long);
            return EncodingUtils.encodeLittleEndian((Long) value);
        }

        @Override
        public int sizeOf(Object value) {
            com.palantir.logsafe.Preconditions.checkArgument(value instanceof Long);
            return 8;
        }

        @Override
        public String getPersistCode(String variableName) {
            return "EncodingUtils.encodeLittleEndian(" + variableName + ")";
        }

        @Override
        public boolean supportsRangeScans() {
            return false;
        }
    },
    /**
     * This value type supports range scans.  Sequential numbers will be written next to each other.
     * This encoding is for 256 bits.  It is always 32 bytes, and treats it as unsigned.
     * It was made for hashes.
     *
     * Until Sha256Hash gets moved, you'll need to manually import Sha256Hash to your rendered class.
     */
    SHA256HASH {
        @Override
        public int getMaxValueSize() {
            return 32;
        }

        @Override
        public Sha256Hash convertToJava(byte[] value, int offset) {
            return new Sha256Hash(EncodingUtils.get32Bytes(value, offset));
        }

        @Override
        public Pair<String, Integer> convertToString(byte[] value, int offset) {
            int size = 32;
            return Pair.create(PtBytes.encodeBase64String(value, offset, size), 32);
        }

        @Override
        public Pair<String, Integer> convertToJson(byte[] value, int offset) {
            Pair<String, Integer> p = convertToString(value, offset);
            return Pair.create("\"" + p.getLhSide() + "\"", p.getRhSide());
        }

        @Override
        public byte[] convertFromString(String strValue) {
            return PtBytes.decodeBase64(strValue);
        }

        @Override
        public byte[] convertFromJson(String jsonValue) {
            return convertFromString(jsonValue.substring(1, jsonValue.length() - 1));
        }

        @Override
        public Class getJavaClass() {
            return Sha256Hash.class;
        }

        @Override
        public byte[] convertFromJava(Object value) {
            com.palantir.logsafe.Preconditions.checkArgument(value instanceof Sha256Hash);
            return ((Sha256Hash) value).getBytes();
        }

        @Override
        public int sizeOf(Object value) {
            com.palantir.logsafe.Preconditions.checkArgument(value instanceof Sha256Hash);
            return 32;
        }

        @Override
        public String getPersistCode(String variableName) {
            return variableName + ".getBytes()";
        }

        @Override
        public String getHydrateCode(String inputName, String indexName) {
            return "new Sha256Hash(EncodingUtils.get32Bytes(" + inputName + ", " + indexName + "))";
        }

        @Override
        public String getFlippedHydrateCode(String inputName, String indexName) {
            return "new Sha256Hash(EncodingUtils.flipAllBitsInPlace(EncodingUtils.get32Bytes(" + inputName + ", "
                    + indexName + ")))";
        }

        @Override
        public String getHydrateSizeCode(String input) {
            return "32";
        }
    },
    /**
     * This value type DOES NOT support range scans.
     * Strings are sorted first by length and then alphabetically, which is probably not what you expected
     */
    VAR_STRING {
        @Override
        public Pair<String, Integer> convertToString(byte[] value, int offset) {
            String str = EncodingUtils.decodeVarString(value, offset);
            return Pair.create(str, EncodingUtils.sizeOfVarString(str));
        }

        @Override
        public String convertToJava(byte[] value, int offset) {
            return EncodingUtils.decodeVarString(value, offset);
        }

        @Override
        public Pair<String, Integer> convertToJson(byte[] value, int offset) {
            Pair<String, Integer> p = convertToString(value, offset);
            return Pair.create(ValueType.writeJson(p.getLhSide()), p.getRhSide());
        }

        @Override
        public byte[] convertFromString(String strValue) {
            return EncodingUtils.encodeVarString(strValue);
        }

        @Override
        public byte[] convertFromJson(String jsonValue) {
            return convertFromString(ValueType.readJson(jsonValue, String.class));
        }

        @Override
        public Class getJavaClass() {
            return String.class;
        }

        @Override
        public String getHydrateCode(String inputName, String indexName) {
            return "EncodingUtils.decodeVarString(" + inputName + ", " + indexName + ")";
        }

        @Override
        public String getFlippedHydrateCode(String inputName, String indexName) {
            return "EncodingUtils.decodeFlippedVarString(" + inputName + ", " + indexName + ")";
        }

        @Override
        public boolean supportsRangeScans() {
            return false;
        }

        @Override
        public String getHydrateSizeCode(String variableName) {
            return "EncodingUtils.sizeOfVarString(" + variableName + ")";
        }

        @Override
        public byte[] convertFromJava(Object value) {
            com.palantir.logsafe.Preconditions.checkArgument(value instanceof String);
            return EncodingUtils.encodeVarString((String) value);
        }

        @Override
        public int sizeOf(Object value) {
            com.palantir.logsafe.Preconditions.checkArgument(value instanceof String);
            return EncodingUtils.sizeOfVarString((String) value);
        }

        @Override
        public String getPersistCode(String variableName) {
            return "EncodingUtils.encodeVarString(" + variableName + ")";
        }
    },
    STRING {
        @Override
        public Pair<String, Integer> convertToString(byte[] value, int offset) {
            int size = value.length - offset;
            return Pair.create(PtBytes.toString(value, offset, size), size);
        }

        @Override
        public String convertToJava(byte[] value, int offset) {
            return PtBytes.toString(value, offset, value.length - offset);
        }

        @Override
        public Pair<String, Integer> convertToJson(byte[] value, int offset) {
            Pair<String, Integer> p = convertToString(value, offset);
            return Pair.create(ValueType.writeJson(p.getLhSide()), p.getRhSide());
        }

        @Override
        public byte[] convertFromString(String strValue) {
            return PtBytes.toBytes(strValue);
        }

        @Override
        public byte[] convertFromJson(String jsonValue) {
            return convertFromString(ValueType.readJson(jsonValue, String.class));
        }

        @Override
        public Class getJavaClass() {
            return String.class;
        }

        @Override
        public String getHydrateCode(String inputName, String indexName) {
            return "PtBytes.toString(" + inputName + ", " + indexName + ", " + inputName + ".length" + "-" + indexName
                    + ")";
        }

        @Override
        public String getFlippedHydrateCode(String inputName, String indexName) {
            // All bits after the index can be flipped because string is always the last thing
            return "PtBytes.toString(EncodingUtils.flipAllBitsInPlace(" + inputName + ", " + indexName + "), "
                    + indexName + ", " + inputName + ".length" + "-" + indexName + ")";
        }

        @Override
        public String getHydrateSizeCode(String variableName) {
            // This value doesn't matter because string is always the last thing
            return "0";
        }

        @Override
        public byte[] convertFromJava(Object value) {
            com.palantir.logsafe.Preconditions.checkArgument(value instanceof String);
            return PtBytes.toBytes((String) value);
        }

        @Override
        public int sizeOf(Object value) {
            com.palantir.logsafe.Preconditions.checkArgument(value instanceof String);
            // This value doesn't matter because string is always the last thing
            return 0;
        }

        @Override
        public String getPersistCode(String variableName) {
            return "PtBytes.toBytes(" + variableName + ")";
        }
    },
    BLOB {
        @Override
        public Pair<String, Integer> convertToString(byte[] value, int offset) {
            int size = value.length - offset;
            return Pair.create(PtBytes.encodeBase64String(value, offset, size), size);
        }

        @Override
        public Pair<String, Integer> convertToJson(byte[] value, int offset) {
            Pair<String, Integer> p = convertToString(value, offset);
            return Pair.create("\"" + p.getLhSide() + "\"", p.getRhSide());
        }

        @Override
        public byte[] convertToJava(byte[] value, int offset) {
            return EncodingUtils.getBytesFromOffsetToEnd(value, offset);
        }

        @Override
        public byte[] convertFromString(String strValue) {
            return PtBytes.decodeBase64(strValue);
        }

        @Override
        public byte[] convertFromJson(String jsonValue) {
            return convertFromString(jsonValue.substring(1, jsonValue.length() - 1));
        }

        @Override
        public Class getJavaClass() {
            return byte[].class;
        }

        @Override
        public byte[] convertFromJava(Object value) {
            com.palantir.logsafe.Preconditions.checkArgument(value instanceof byte[]);
            return (byte[]) value;
        }

        @Override
        public int sizeOf(Object value) {
            com.palantir.logsafe.Preconditions.checkArgument(value instanceof byte[]);
            // This value doesn't matter because blob is always the last thing
            return 0;
        }

        @Override
        public String getPersistCode(String variableName) {
            return variableName;
        }

        @Override
        public String getHydrateCode(String inputName, String indexName) {
            return "EncodingUtils.getBytesFromOffsetToEnd(" + inputName + ", " + indexName + ")";
        }

        @Override
        public String getFlippedHydrateCode(String inputName, String indexName) {
            // All bits after the index can be flipped because blob is always the last thing
            return "EncodingUtils.flipAllBitsInPlace(EncodingUtils.getBytesFromOffsetToEnd(" + inputName + ", "
                    + indexName + "))";
        }

        @Override
        public String getHydrateSizeCode(String variableName) {
            // This value doesn't matter because blob is always the last thing
            return "0";
        }
    },
    /**
     * This value type DOES NOT support range scans.
     */
    SIZED_BLOB {
        @Override
        public Pair<String, Integer> convertToString(byte[] value, int offset) {
            byte[] bytes = EncodingUtils.decodeSizedBytes(value, offset);
            return Pair.create(PtBytes.encodeBase64String(bytes), EncodingUtils.sizeOfSizedBytes(bytes));
        }

        @Override
        public Pair<String, Integer> convertToJson(byte[] value, int offset) {
            Pair<String, Integer> p = convertToString(value, offset);
            return Pair.create("\"" + p.getLhSide() + "\"", p.getRhSide());
        }

        @Override
        public byte[] convertToJava(byte[] value, int offset) {
            return EncodingUtils.decodeSizedBytes(value, offset);
        }

        @Override
        public byte[] convertFromString(String strValue) {
            return EncodingUtils.encodeSizedBytes(PtBytes.decodeBase64(strValue));
        }

        @Override
        public byte[] convertFromJson(String jsonValue) {
            return convertFromString(jsonValue.substring(1, jsonValue.length() - 1));
        }

        @Override
        public Class getJavaClass() {
            return byte[].class;
        }

        @Override
        public byte[] convertFromJava(Object value) {
            com.palantir.logsafe.Preconditions.checkArgument(value instanceof byte[]);
            return EncodingUtils.encodeSizedBytes((byte[]) value);
        }

        @Override
        public int sizeOf(Object value) {
            com.palantir.logsafe.Preconditions.checkArgument(value instanceof byte[]);
            return EncodingUtils.sizeOfSizedBytes((byte[]) value);
        }

        @Override
        public String getPersistCode(String variableName) {
            return "EncodingUtils.encodeSizedBytes(" + variableName + ")";
        }

        @Override
        public String getHydrateCode(String inputName, String indexName) {
            return "EncodingUtils.decodeSizedBytes(" + inputName + ", " + indexName + ")";
        }

        @Override
        public String getFlippedHydrateCode(String inputName, String indexName) {
            return "EncodingUtils.decodeFlippedSizedBytes(" + inputName + ", " + indexName + ")";
        }

        @Override
        public boolean supportsRangeScans() {
            return false;
        }

        @Override
        public String getHydrateSizeCode(String variableName) {
            return "EncodingUtils.sizeOfSizedBytes(" + variableName + ")";
        }
    },
    NULLABLE_FIXED_LONG {
        @Override
        public int getMaxValueSize() {
            return 9;
        }

        @Override
        public Long convertToJava(byte[] value, int offset) {
            return EncodingUtils.decodeNullableFixedLong(value, offset);
        }

        @Override
        public Pair<String, Integer> convertToString(byte[] value, int offset) {
            return Pair.create(String.valueOf(EncodingUtils.decodeNullableFixedLong(value, offset)), 9);
        }

        @Override
        public Pair<String, Integer> convertToJson(byte[] value, int offset) {
            return convertToString(value, offset);
        }

        @Override
        public byte[] convertFromString(String strValue) {
            return convertFromJava(strValue == null ? null : Long.parseLong(strValue));
        }

        @Override
        public byte[] convertFromJson(String jsonValue) {
            return convertFromString(jsonValue);
        }

        @Override
        public Class getJavaClass() {
            return Long.class;
        }

        @Override
        public String getHydrateCode(String inputName, String indexName) {
            return String.format("EncodingUtils.decodeNullableFixedLong(%s,%s)", inputName, indexName);
        }

        @Override
        public String getFlippedHydrateCode(String inputName, String indexName) {
            return String.format("EncodingUtils.decodeFlippedNullableFixedLong(%s,%s)", inputName, indexName);
        }

        @Override
        public String getHydrateSizeCode(String input) {
            return "9";
        }

        @Override
        public byte[] convertFromJava(Object value) {
            com.palantir.logsafe.Preconditions.checkArgument(value == null || value instanceof Long);
            return EncodingUtils.encodeNullableFixedLong((Long) value);
        }

        @Override
        public int sizeOf(Object value) {
            com.palantir.logsafe.Preconditions.checkArgument(value == null || value instanceof Long);
            return 9;
        }

        @Override
        public String getPersistCode(String variableName) {
            return String.format("EncodingUtils.encodeNullableFixedLong(%s)", variableName);
        }
    },
    UUID {
        @Override
        public java.util.UUID convertToJava(byte[] value, int offset) {
            com.palantir.logsafe.Preconditions.checkArgument(
                    offset + 16 <= value.length, "Field does not contain enough remaining bytes to hold a UUID");
            return EncodingUtils.decodeUUID(value, offset);
        }

        @Override
        public byte[] convertFromJava(Object value) {
            com.palantir.logsafe.Preconditions.checkArgument(value == null || value instanceof java.util.UUID);
            return EncodingUtils.encodeUUID((java.util.UUID) value);
        }

        @Override
        public Pair<String, Integer> convertToString(byte[] value, int offset) {
            return Pair.create(convertToJava(value, offset).toString(), 16);
        }

        @Override
        public byte[] convertFromString(String strValue) {
            return convertFromJava(java.util.UUID.fromString(strValue));
        }

        @Override
        public Pair<String, Integer> convertToJson(byte[] value, int offset) {
            return Pair.create(ValueType.writeJson(convertToJava(value, offset).toString()), 16);
        }

        @Override
        public byte[] convertFromJson(String jsonValue) {
            return convertFromString(ValueType.readJson(jsonValue, String.class));
        }

        @Override
        public int sizeOf(Object value) {
            return 16;
        }

        @Override
        public Class getJavaClass() {
            return java.util.UUID.class;
        }

        @Override
        public String getPersistCode(String variableName) {
            return String.format("EncodingUtils.encodeUUID(%s)", variableName);
        }

        @Override
        public String getHydrateCode(String inputName, String indexName) {
            return String.format("EncodingUtils.decodeUUID(%s, %s)", inputName, indexName);
        }

        @Override
        public String getFlippedHydrateCode(String inputName, String indexName) {
            return String.format("EncodingUtils.decodeFlippedUUID(%s, %s)", inputName, indexName);
        }

        @Override
        public String getHydrateSizeCode(String variableName) {
            return "16";
        }
    };

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public abstract Object convertToJava(byte[] value, int offset);

    public abstract Pair<String, Integer> convertToJson(byte[] value, int offset);

    public abstract Pair<String, Integer> convertToString(byte[] value, int offset);

    public abstract byte[] convertFromString(String strValue);

    public abstract byte[] convertFromJava(Object value);

    public abstract byte[] convertFromJson(String jsonValue);

    public abstract int sizeOf(Object value);

    public String convertToJson(byte[] value) {
        return convertToJson(value, 0).getLhSide();
    }

    public String convertToString(byte[] value) {
        return convertToString(value, 0).getLhSide();
    }

    public boolean supportsRangeScans() {
        return true;
    }

    public abstract Class getJavaClass();

    public Class getJavaObjectClass() {
        return getJavaClass();
    }

    public String getJavaClassName() {
        return getJavaClass().getSimpleName();
    }

    public String getJavaObjectClassName() {
        return getJavaObjectClass().getSimpleName();
    }

    public abstract String getPersistCode(String variableName);

    public abstract String getHydrateCode(String inputName, String indexName);

    public abstract String getFlippedHydrateCode(String inputName, String indexName);

    public abstract String getHydrateSizeCode(String variableName);

    public int getMaxValueSize() {
        return Integer.MAX_VALUE;
    }

    public TableMetadataPersistence.ValueType persistToProto() {
        return TableMetadataPersistence.ValueType.valueOf(name());
    }

    public static ValueType hydrateFromProto(TableMetadataPersistence.ValueType message) {
        return valueOf(message.name());
    }

    private static <T> T readJson(String json, Class<T> clazz) {
        try {
            return OBJECT_MAPPER.readValue(json, clazz);
        } catch (IOException e) {
            throw new SafeIllegalArgumentException("{} must be a JSON string", UnsafeArg.of("json", json));
        }
    }

    private static String writeJson(Object value) {
        try {
            return OBJECT_MAPPER.writeValueAsString(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
