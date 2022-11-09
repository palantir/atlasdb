/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock;

import com.google.common.collect.Range;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.nio.charset.StandardCharsets;

public final class AtlasLockDescriptorRanges {
    private AtlasLockDescriptorRanges() {
        // no
    }

    public static Range<LockDescriptor> fullTable(String qualifiedTableName) {
        byte[] tableNameBytes = bytesForTableName(qualifiedTableName);
        return Range.closedOpen(
                new LockDescriptor(tableNameBytes),
                new LockDescriptor(createExclusiveEndNameForPrefixScan(tableNameBytes)));
    }

    public static Range<LockDescriptor> rowPrefix(String qualifiedTableName, byte[] prefix) {
        LockDescriptor start = AtlasRowLockDescriptor.of(qualifiedTableName, prefix);
        return Range.closedOpen(start, new LockDescriptor(createExclusiveEndNameForPrefixScan(start.getBytes())));
    }

    public static Range<LockDescriptor> rowRange(String qualifiedTableName, byte[] startInc, byte[] endExc) {
        return Range.closedOpen(
                AtlasRowLockDescriptor.of(qualifiedTableName, startInc),
                AtlasRowLockDescriptor.of(qualifiedTableName, endExc));
    }

    // If the row is "row", then nextRow will be "row{01}" (UTF-8 row followed by the byte 01 = {72}{6F}{77}{01})
    // This will match descriptors from "row" to "row{00}{FF}"
    // 00 is used as a delimiter in cell lock descriptors, separating row bytes from column bytes.
    // So we will match the RowLockDescriptor, as well as any CellLockDescriptor with the matching table and row.
    // This is done so that when a client registers a watch for a given row, they get lock and unlock events when
    // cell-level locks are taken out.
    public static Range<LockDescriptor> exactRow(String qualifiedTableName, byte[] row) {
        LockDescriptor descriptor = AtlasRowLockDescriptor.of(qualifiedTableName, row);
        byte[] nextRow = createExclusiveEndNameWithZeroByteForPrefixScan(row);
        LockDescriptor nextRowDescriptor = AtlasRowLockDescriptor.of(qualifiedTableName, nextRow);
        return Range.closedOpen(descriptor, nextRowDescriptor);
    }

    public static Range<LockDescriptor> exactCell(String qualifiedTableName, byte[] row, byte[] col) {
        LockDescriptor descriptor = AtlasCellLockDescriptor.of(qualifiedTableName, row, col);
        return Range.closed(descriptor, descriptor);
    }

    private static byte[] bytesForTableName(String tableName) {
        return tableName.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] createExclusiveEndNameWithZeroByteForPrefixScan(byte[] prefix) {
        byte[] prefixWithZero = new byte[prefix.length + 1];
        System.arraycopy(prefix, 0, prefixWithZero, 0, prefix.length);
        prefixWithZero[prefix.length] = 1;
        return prefixWithZero;
    }

    private static byte[] createExclusiveEndNameForPrefixScan(byte[] prefix) {
        for (int i = prefix.length - 1; i >= 0; i--) {
            if ((prefix[i] & 0xff) != 0xff) {
                byte[] ret = new byte[i + 1];
                System.arraycopy(prefix, 0, ret, 0, i + 1);
                ret[i]++;
                return ret;
            }
        }
        // this is unreachable when a part of prefix was created from a String
        throw new SafeIllegalArgumentException("The prefix must be constructed using a valid table name.");
    }
}
