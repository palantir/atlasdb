// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.keyvalue.leveldb.impl;

import java.io.IOException;
import java.util.Arrays;

import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedMap.Builder;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.Throwables;


abstract class RangeIterator<T> extends AbstractIterator<RowResult<T>> implements ClosableIterator<RowResult<T>> {
    private static final Logger log = LoggerFactory.getLogger(RangeIterator.class);

    protected final DBIterator it;
    private final RangeRequest request;
    private final byte[] endRowPrefix;
    protected final long timestampExclusive;
    protected final byte[] tablePrefix;


    RangeIterator(DBIterator it, byte[] tablePrefix, RangeRequest range, long endTimestamp) {
        this.it = it;
        this.request = range;
        this.endRowPrefix = LevelDbKeyValueUtils.getKeyPrefixWithRow(tablePrefix, range.getEndExclusive());
        this.timestampExclusive = endTimestamp;
        this.tablePrefix = tablePrefix;
    }


    @Override
    protected RowResult<T> computeNext() {
        try {
            while (it.hasNext() && PtBytes.startsWith(it.peekNext().getKey(), tablePrefix)) {
                if (LevelDbKeyValueUtils.hasMovedPastPrefix(it.peekNext().getKey(), endRowPrefix)) {
                    break;
                }
                final byte[] rowName =
                    LevelDbKeyValueUtils.parseCellAndTs(it.peekNext().getKey(), tablePrefix).lhSide.getRowName();
                final RowResult<T> rowResult = processRow(rowName);
                if (rowResult != null
                        && LevelDbKeyValueUtils.isInRange(rowName, request.getStartInclusive(), request.getEndExclusive())) {
                    return rowResult;
                }
            }
            return endOfData();
        } catch (DBException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }


    private RowResult<T> processRow(byte[] rowName) throws DBException {
        final Builder<byte[], T> builder = ImmutableSortedMap.orderedBy(UnsignedBytes.lexicographicalComparator());
        while (it.hasNext()) {
            final Cell cell = LevelDbKeyValueUtils.parseCellAndTs(it.peekNext().getKey(), tablePrefix).lhSide;
            if (!Arrays.equals(rowName, cell.getRowName())) {
                // This is actually a different row
                break;
            }
            final T v = processCell(cell);
            if (v != null && request.containsColumn(cell.getColumnName())) {
                builder.put(cell.getColumnName(), v);
            }
        }
        final ImmutableSortedMap<byte[], T> map = builder.build();
        if (map.isEmpty()) {
            return null;
        } else {
            return RowResult.create(rowName, map);
        }
    }

    protected abstract T processCell(Cell cell) throws DBException;

    @Override
    public void close() {
        try {
            it.close();
        } catch (IOException e) {
            log.warn("close failed", e);
        }
    }
}
