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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.SetMultimap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.common.base.ClosableIterator;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;
import java.util.Arrays;

final class TimestampsByCellResultWithToken {
    private byte[] currentRow = null;
    private byte[] currentCol = null;
    private PeekingIterator<AgnosticLightResultRow> iterator;

    final SetMultimap<Cell, Long> entries;
    private SetMultimap<Cell, Long> rowBuffer;
    private boolean moreResults = false;
    private Token token = Token.INITIAL;
    private final boolean reverse;

    private TimestampsByCellResultWithToken(ClosableIterator<AgnosticLightResultRow> iterator, boolean reverse) {
        entries = HashMultimap.create();
        rowBuffer = HashMultimap.create();
        this.iterator = Iterators.peekingIterator(iterator);
        this.reverse = reverse;
    }

    static TimestampsByCellResultWithToken create(ClosableIterator<AgnosticLightResultRow> iterator,
            Token oldToken,
            long batchSize,
            boolean reverse) {
        return new TimestampsByCellResultWithToken(iterator, reverse)
                .moveForward(oldToken)
                .getBatchOfTimestamps(batchSize)
                .checkNextEntryAndCreateToken();
    }

    /**
     * @param oldToken token from previous page, specifying if we have already processed some entries from the current
     * row and should therefore skip them. If oldToken.shouldSkip() is true, we iterate until the end or the first
     * result that is either:
     *  1. In another row
     *  2. In a greater column
     */
    private TimestampsByCellResultWithToken moveForward(Token oldToken) {
        boolean skipping = oldToken.shouldSkip();
        while (skipping && iterator.hasNext()) {
            AgnosticLightResultRow nextResult = iterator.peek();
            if (finishedSkipping(oldToken, nextResult)) {
                skipping = false;
            } else {
                iterator.next();
            }
        }
        entries.putAll(rowBuffer);
        return this;
    }

    private boolean finishedSkipping(Token oldToken, AgnosticLightResultRow next) {
        return !Arrays.equals(next.getBytes(DbKvs.ROW), oldToken.row())
                || compareColumns(oldToken, next) > 0;
    }

    private static int compareColumns(Token oldToken, AgnosticLightResultRow nextResult) {
        return UnsignedBytes.lexicographicalComparator().compare(nextResult.getBytes(DbKvs.COL), oldToken.col());
    }

    private TimestampsByCellResultWithToken getBatchOfTimestamps(long batchSize) {
        while (iterator.hasNext() && entries.size() + rowBuffer.size() < batchSize) {
            AgnosticLightResultRow cellResult = iterator.next();
            store(cellResult);
        }
        return this;
    }

    private void store(AgnosticLightResultRow cellResult) {
        byte[] newRow = cellResult.getBytes(DbKvs.ROW);
        currentCol = cellResult.getBytes(DbKvs.COL);
        long timestamp = cellResult.getLong(DbKvs.TIMESTAMP);
        if (!Arrays.equals(currentRow, newRow)) {
            flushRowBuffer();
            currentRow = newRow;
        }
        Cell cell = Cell.create(currentRow, currentCol);
        rowBuffer.put(cell, timestamp);
    }

    private void flushRowBuffer() {
        entries.putAll(rowBuffer);
        rowBuffer.clear();
    }

    private TimestampsByCellResultWithToken checkNextEntryAndCreateToken() {
        boolean singleRow = finishCellIfNoRowsYet();
        if (iterator.hasNext()) {
            moreResults = true;
            AgnosticLightResultRow nextEntry = iterator.peek();
            if (Arrays.equals(nextEntry.getBytes(DbKvs.ROW), currentRow)) {
                token = ImmutableToken.builder()
                        .row(currentRow)
                        .col(currentCol)
                        .shouldSkip(singleRow)
                        .build();
            } else {
                flushRowBuffer();
                token = ImmutableToken.builder().row(nextEntry.getBytes(DbKvs.ROW)).shouldSkip(false).build();
            }
        } else {
            flushRowBuffer();
            if (currentRow != null) {
                byte[] nextRow = RangeRequests.getNextStartRowUnlessTerminal(reverse, currentRow);
                if (nextRow != null) {
                    moreResults = true;
                    token = ImmutableToken.builder().row(nextRow).shouldSkip(false).build();
                }
            }
        }
        return this;
    }

    private boolean finishCellIfNoRowsYet() {
        if (entries.size() == 0) {
            flushRowBuffer();
            while (currentCellHasEntriesLeft()) {
                long timestamp = iterator.next().getLong(DbKvs.TIMESTAMP);
                entries.put(Cell.create(currentRow, currentCol), timestamp);
            }
            return true;
        }
        return false;
    }

    private boolean currentCellHasEntriesLeft() {
        return iterator.hasNext()
                && Arrays.equals(iterator.peek().getBytes(DbKvs.ROW), currentRow)
                && Arrays.equals(iterator.peek().getBytes(DbKvs.COL), currentCol);
    }

    public boolean mayHaveMoreResults() {
        return moreResults;
    }

    public Token getToken() {
        return token;
    }
}
