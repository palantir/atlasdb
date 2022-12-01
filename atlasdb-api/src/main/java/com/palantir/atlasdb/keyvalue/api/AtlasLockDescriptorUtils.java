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

import com.google.common.primitives.Bytes;
import com.google.protobuf.ByteString;
import com.palantir.lock.LockDescriptor;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.immutables.value.Value;

public final class AtlasLockDescriptorUtils {
    private static final SafeLogger log = SafeLoggerFactory.get(AtlasLockDescriptorUtils.class);
    private static final int CELL_BLOWUP_THRESHOLD = 100;

    private AtlasLockDescriptorUtils() {
        // NOPE
    }

    public static List<CellReference> candidateCells(LockDescriptor lockDescriptor) {
        Optional<TableRefAndRemainder> tableRef = tryParseTableRef(lockDescriptor);
        //noinspection OptionalIsPresent - avoid Optional#map to avoid Optional allocations
        if (tableRef.isEmpty()) {
            return Collections.emptyList();
        }
        return candidateCells(tableRef.get());
    }

    public static List<CellReference> candidateCells(TableRefAndRemainder parsedLockDescriptor) {
        TableReference tableRef = parsedLockDescriptor.tableRef();
        ByteString remainingBytes = parsedLockDescriptor.remainder();

        List<CellReference> candidateCells = IntStream.range(1, remainingBytes.size() - 1)
                .filter(index -> isZeroDelimiterIndex(remainingBytes, index))
                .mapToObj(index -> createCellFromByteString(remainingBytes, index))
                .map(cell -> CellReference.of(tableRef, cell))
                .collect(Collectors.toList());

        if (candidateCells.size() > CELL_BLOWUP_THRESHOLD) {
            log.warn(
                    "Lock descriptor produced a large number of candidate cells - this is due to the descriptor "
                            + "containing many zero bytes. If this message is logged frequently, this table may be "
                            + "inappropriate for caching",
                    SafeArg.of("candidateCellSize", candidateCells.size()),
                    UnsafeArg.of("lockDescriptorBytes", remainingBytes));
        }

        return candidateCells;
    }

    public static Optional<TableRefAndRemainder> tryParseTableRef(LockDescriptor lockDescriptor) {
        byte[] rawBytes = lockDescriptor.getBytes();

        int endOfTableName = Bytes.indexOf(rawBytes, (byte) 0);
        if (endOfTableName == -1) {
            return Optional.empty();
        }
        String fullyQualifiedName = new String(rawBytes, 0, endOfTableName, StandardCharsets.UTF_8);
        TableReference tableRef = TableReference.createFromFullyQualifiedName(fullyQualifiedName);
        ByteString remainingBytes =
                ByteString.copyFrom(rawBytes, endOfTableName + 1, rawBytes.length - (endOfTableName + 1));
        return Optional.of(ImmutableTableRefAndRemainder.of(tableRef, remainingBytes));
    }

    private static boolean isZeroDelimiterIndex(ByteString remainingBytes, int candidateIndex) {
        return remainingBytes.byteAt(candidateIndex) == 0;
    }

    private static Cell createCellFromByteString(ByteString remainingBytes, int zeroDelimiterIndex) {
        byte[] row = remainingBytes.substring(0, zeroDelimiterIndex).toByteArray();
        byte[] col = remainingBytes
                .substring(zeroDelimiterIndex + 1, remainingBytes.size())
                .toByteArray();
        return Cell.create(row, col);
    }

    @Value.Immutable
    public interface TableRefAndRemainder {
        @Value.Parameter
        TableReference tableRef();

        @Value.Parameter
        ByteString remainder();
    }
}
