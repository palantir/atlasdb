/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.immutables.value.Value;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Bytes;
import com.palantir.lock.LockDescriptor;

import okio.ByteString;

public final class AtlasLockDescriptorUtils {
    private static final byte[] ZERO_ARRAY = new byte[] {0};

    private AtlasLockDescriptorUtils() {
        // NOPE
    }

    public static List<CellReference> candidateCells(LockDescriptor lockDescriptor) {
        Optional<TableRefAndRemainder> tableRefAndRemainder = tryParseTableRef(lockDescriptor);
        if (!tableRefAndRemainder.isPresent()) {
            return ImmutableList.of();
        }

        TableReference tableRef = tableRefAndRemainder.get().tableRef();
        ByteString remainingBytes = tableRefAndRemainder.get().remainder();

        int lookupFrom = 0;
        int nextCandidate;
        List<Cell> cells = new ArrayList<>();
        while ((nextCandidate = remainingBytes.indexOf(ZERO_ARRAY, lookupFrom)) != -1) {
            if (nextCandidate > 0 && nextCandidate < remainingBytes.size() - 2) {
                byte[] row = remainingBytes.substring(0, nextCandidate).toByteArray();
                byte[] col = remainingBytes.substring(nextCandidate + 1, remainingBytes.size()).toByteArray();
                cells.add(Cell.create(row, col));
            }
            lookupFrom = nextCandidate + 1;
        }
        return cells.stream().map(cell -> CellReference.of(tableRef, cell)).collect(Collectors.toList());
    }

    public static Optional<TableRefAndRemainder> tryParseTableRef(LockDescriptor lockDescriptor) {
        byte[] rawBytes = lockDescriptor.getBytes();

        int endOfTableName = Bytes.indexOf(rawBytes, (byte) 0);
        if (endOfTableName == -1) {
            return Optional.empty();
        }
        String fullyQualifiedName = new String(rawBytes, 0, endOfTableName);
        TableReference tableRef = TableReference.createFromFullyQualifiedName(fullyQualifiedName);
        ByteString remainingBytes = ByteString.of(rawBytes, endOfTableName + 1, rawBytes.length - 1 - endOfTableName);
        return Optional.of(ImmutableTableRefAndRemainder.of(tableRef, remainingBytes));
    }

    @Value.Immutable
    public interface TableRefAndRemainder {
        @Value.Parameter
        TableReference tableRef();
        @Value.Parameter
        ByteString remainder();
    }
}
