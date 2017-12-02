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
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.transaction.api.Transaction;

public class OffsetManagerImpl implements OffsetManager {
    private static final long MAX_ENTRIES_PER_BUCKET = 10_000;

    private final TableReference queueReadOffsetTable;
    private final TableReference queueWriteOffsetTable;
    private final byte[] queueKey;

    public OffsetManagerImpl(
            TableReference queueReadOffsetTable,
            TableReference queueWriteOffsetTable, byte[] queueKey) {
        this.queueReadOffsetTable = queueReadOffsetTable;
        this.queueWriteOffsetTable = queueWriteOffsetTable;
        this.queueKey = queueKey;
    }

    @Override
    public QueueQuery translateWriteQuery(Transaction tx) {
        return translateOffsetToQuery(queueKey, getCurrentWriteOffset(tx, queueKey));
    }

    @Override
    public Optional<QueueQuery> translateReadQuery(Transaction tx) {
        long readOffset = getCurrentReadOffset(tx, queueKey);
        long writeOffset = getCurrentWriteOffset(tx, queueKey);

        if (readOffset == writeOffset) {
            return Optional.empty();
        }
        return Optional.of(translateOffsetToQuery(queueKey, readOffset));
    }

    @Override
    public void updateWriteOffsetPast(Transaction tx, byte[] oldOffset) {
        tx.put(queueWriteOffsetTable,
                ImmutableMap.of(getOffsetCellFromQueueKey(queueKey), incrementUnsignedVarLong(oldOffset)));
    }

    @Override
    public void updateReadOffsetPast(Transaction tx, byte[] oldOffset) {
        tx.put(queueReadOffsetTable,
                ImmutableMap.of(getOffsetCellFromQueueKey(queueKey), incrementUnsignedVarLong(oldOffset)));
    }

    private QueueQuery translateOffsetToQuery(byte[] key, long offset) {
        long bucketNumber = offset / MAX_ENTRIES_PER_BUCKET;
        return ImmutableQueueQuery.of(EncodingUtils.add(key, EncodingUtils.encodeUnsignedVarLong(bucketNumber)),
                EncodingUtils.encodeUnsignedVarLong(offset));
    }

    private long getCurrentWriteOffset(Transaction tx, byte[] key) {
        return getOffsetFromTable(tx, key, queueWriteOffsetTable);
    }

    private long getCurrentReadOffset(Transaction tx, byte[] key) {
        return getOffsetFromTable(tx, key, queueReadOffsetTable);
    }

    private static byte[] incrementUnsignedVarLong(byte[] varLongBytes) {
        return EncodingUtils.encodeUnsignedVarLong(EncodingUtils.decodeUnsignedVarLong(varLongBytes) + 1);
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
