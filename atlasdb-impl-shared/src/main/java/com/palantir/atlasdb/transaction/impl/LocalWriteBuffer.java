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

package com.palantir.atlasdb.transaction.impl;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.lock.watch.ChangeMetadata;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class LocalWriteBuffer {
    private static final SafeLogger log = SafeLoggerFactory.get(LocalWriteBuffer.class);

    private final ConcurrentMap<TableReference, ConcurrentNavigableMap<Cell, byte[]>> writesByTable =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<TableReference, ConcurrentMap<Cell, ChangeMetadata>> metadataByTable =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<TableReference, Object> locksByTable = new ConcurrentHashMap<>();
    private final AtomicLong byteCount = new AtomicLong();

    public void putLocalWritesAndMetadata(
            TableReference tableRef, Map<Cell, byte[]> values, Map<Cell, ChangeMetadata> metadata) {
        ConcurrentMap<Cell, byte[]> writes = getLocalWritesForTable(tableRef);
        ConcurrentMap<Cell, ChangeMetadata> metadataForWrites = getChangeMetadataForWritesToTable(tableRef);
        int numMetadataWritten = 0;
        synchronized (getLockForTable(tableRef)) {
            for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
                byte[] val = MoreObjects.firstNonNull(e.getValue(), PtBytes.EMPTY_BYTE_ARRAY);
                Cell cell = e.getKey();
                byte[] oldVal = writes.put(cell, val);
                // If we are not writing metadata for a value, we have to remove any previously stored metadata since
                // it may not be valid for the new value.
                if (metadata.containsKey(cell)) {
                    metadataForWrites.put(cell, metadata.get(cell));
                    numMetadataWritten++;
                } else {
                    metadataForWrites.remove(cell);
                }
                long toAdd = val.length + Cells.getApproxSizeOfCell(cell);
                long toSubtract = oldVal != null ? oldVal.length + Cells.getApproxSizeOfCell(cell) : 0;
                long newByteCount = byteCount.addAndGet(toAdd - toSubtract);
                if (newByteCount >= TransactionConstants.WARN_LEVEL_FOR_QUEUED_BYTES
                        && newByteCount - toAdd < TransactionConstants.WARN_LEVEL_FOR_QUEUED_BYTES) {
                    log.warn(
                            "A single transaction has put quite a few bytes: {}. "
                                    + "Enable debug logging for more information",
                            SafeArg.of("numBytes", newByteCount));
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "This exception and stack trace are provided for debugging purposes.",
                                new RuntimeException());
                    }
                }
            }
        }
        if (numMetadataWritten != metadata.size()) {
            Set<Cell> cellsWithOnlyMetadata = Sets.difference(metadata.keySet(), values.keySet());
            throw new SafeIllegalStateException(
                    "Every metadata we put must be associated with a write",
                    UnsafeArg.of("tableRef", tableRef),
                    UnsafeArg.of("cellsWithOnlyMetadata", cellsWithOnlyMetadata));
        }
    }

    public ConcurrentMap<TableReference, ConcurrentNavigableMap<Cell, byte[]>> getLocalWrites() {
        return writesByTable;
    }

    public ConcurrentNavigableMap<Cell, byte[]> getLocalWritesForTable(TableReference tableRef) {
        return writesByTable.computeIfAbsent(tableRef, unused -> new ConcurrentSkipListMap<>());
    }

    public ConcurrentMap<Cell, ChangeMetadata> getChangeMetadataForWritesToTable(TableReference tableRef) {
        return metadataByTable.computeIfAbsent(tableRef, unused -> new ConcurrentHashMap<>());
    }

    public boolean isEmpty() {
        return writesByTable.isEmpty();
    }

    public long getByteCount() {
        return byteCount.get();
    }

    private Object getLockForTable(TableReference tableRef) {
        return locksByTable.computeIfAbsent(tableRef, _unused -> new Object());
    }
}
