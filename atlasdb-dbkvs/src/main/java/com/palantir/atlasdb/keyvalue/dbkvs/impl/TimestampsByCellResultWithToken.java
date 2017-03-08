/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.util.Arrays;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.SetMultimap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.common.base.ClosableIterator;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;

final class TimestampsByCellResultWithToken {
    private static final String ROW = "row_name";
    private static final String COL = "col_name";
    private static final String TIMESTAMP = "ts";

    private byte[] currentRow = null;
    private byte[] currentCol = null;
    private Long currentTimestamp = null;
    private PeekingIterator<AgnosticLightResultRow> iterator;

    final SetMultimap<Cell, Long> entries;
    boolean mayHaveMoreResults = false;
    Token token = Token.INITIAL;

    private TimestampsByCellResultWithToken(ClosableIterator<AgnosticLightResultRow> iterator) {
        entries = HashMultimap.create();
        this.iterator = Iterators.peekingIterator(iterator);
    }

    static TimestampsByCellResultWithToken create(ClosableIterator<AgnosticLightResultRow> iterator,
            Token oldToken,
            long batchSize) {
        return new TimestampsByCellResultWithToken(iterator)
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
     *  3. In the same column, with higher or equal timestamp (to repeat the last timestamp for sweep)
     */
    TimestampsByCellResultWithToken moveForward(Token oldToken) {
        boolean skipping = oldToken.shouldSkip();
        while (skipping && iterator.hasNext()) {
            AgnosticLightResultRow nextResult = iterator.peek();
            if (finishedForwarding(oldToken, nextResult)) {
                skipping = false;
            } else {
                iterator.next();
            }
        }
        return this;
    }

    private boolean finishedForwarding(Token oldToken, AgnosticLightResultRow next) {
        return !Arrays.equals(next.getBytes(ROW), oldToken.row())
                || compareColumns(oldToken, next) > 0
                || (compareColumns(oldToken, next) == 0 && next.getLong(TIMESTAMP) >= oldToken.timestamp());
    }

    private static int compareColumns(Token oldToken, AgnosticLightResultRow nextResult) {
        return UnsignedBytes.lexicographicalComparator().compare(nextResult.getBytes(COL), oldToken.col());
    }

    TimestampsByCellResultWithToken getBatchOfTimestamps(long batchSize) {
        while (iterator.hasNext() && entries.size() < batchSize) {
            AgnosticLightResultRow cellResult = iterator.next();
            store(cellResult);
        }
        return this;
    }

    private void store(AgnosticLightResultRow cellResult) {
        currentRow = cellResult.getBytes(ROW);
        currentCol = cellResult.getBytes(COL);
        currentTimestamp = cellResult.getLong(TIMESTAMP);
        Cell cell = Cell.create(currentRow, currentCol);
        entries.put(cell, currentTimestamp);
    }

    TimestampsByCellResultWithToken checkNextEntryAndCreateToken() {
        if (iterator.hasNext()) {
            mayHaveMoreResults = true;
            AgnosticLightResultRow nextEntry = iterator.peek();
            if (Arrays.equals(nextEntry.getBytes(ROW), currentRow)) {
                token = ImmutableToken.builder()
                        .row(currentRow)
                        .col(currentCol)
                        .timestamp(currentTimestamp)
                        .shouldSkip(true)
                        .build();
            } else {
                token = ImmutableToken.builder().row(nextEntry.getBytes(ROW)).shouldSkip(false).build();
            }
        }
        return this;
    }
}
