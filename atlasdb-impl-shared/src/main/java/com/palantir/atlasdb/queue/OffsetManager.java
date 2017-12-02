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

package com.palantir.atlasdb.queue;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.transaction.api.Transaction;

public class OffsetManager {
    private static final long MAX_ENTRIES_PER_BUCKET = 10_000;

    private final TableReference queueReadOffsetTable;
    private final TableReference queueWriteOffsetTable;

    public OffsetManager(
            TableReference queueReadOffsetTable,
            TableReference queueWriteOffsetTable) {
        this.queueReadOffsetTable = queueReadOffsetTable;
        this.queueWriteOffsetTable = queueWriteOffsetTable;
    }

    public Map<byte[], Long> translateWriteQuery(Transaction tx, byte[] key) {
        return translateOffsetToQuery(key, getCurrentWriteOffset(tx, key));
    }

    public Map<byte[], Long> translateReadQuery(Transaction tx, byte[] key) {
        long readOffset = getCurrentReadOffset(tx, key);
        long writeOffset = getCurrentWriteOffset(tx, key);

        if (readOffset == writeOffset) {
            return ImmutableMap.of();
        }
        return translateOffsetToQuery(key, readOffset);
    }

    private Map<byte[], Long> translateOffsetToQuery(byte[] key, long offset) {
        long bucketNumber = offset / MAX_ENTRIES_PER_BUCKET;
        return ImmutableMap.of(EncodingUtils.add(key, EncodingUtils.encodeUnsignedVarLong(bucketNumber)),
                offset);
    }

    private long getCurrentWriteOffset(Transaction tx, byte[] key) {
        return getOffsetFromTable(tx, key, queueWriteOffsetTable);
    }

    private long getCurrentReadOffset(Transaction tx, byte[] key) {
        return getOffsetFromTable(tx, key, queueReadOffsetTable);
    }

    private static long getOffsetFromTable(Transaction tx, byte[] key, TableReference sourceTable) {
        Cell queueOffsetCell = getOffsetCellFromQueueKey(key);
        Map<Cell, byte[]> bound = tx.get(sourceTable, ImmutableSet.of(queueOffsetCell));
        return bound.containsKey(queueOffsetCell) ?
                EncodingUtils.decodeUnsignedVarLong(bound.get(queueOffsetCell)) :
                0L;
    }

    private static Cell getOffsetCellFromQueueKey(byte[] queueKey) {
        return Cell.create(queueKey, PtBytes.toBytes("o"));
    }
}
