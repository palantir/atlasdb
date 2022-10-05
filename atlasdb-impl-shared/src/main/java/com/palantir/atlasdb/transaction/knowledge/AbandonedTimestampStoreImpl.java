/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.knowledge;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.transaction.encoding.TicketsCellEncodingStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.streams.KeyedStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AbandonedTimestampStoreImpl implements AbandonedTimestampStore {
    // DO NOT change the following without a migration of the known aborted timestamps table!
    // These values were chosen on the basis that unlike in transactions2, values provided are markers and so we can
    // store more values per row (because we don't have a commit timestamp that we also need to store).
    public static final long PARTITIONING_QUANTUM = 50_000_000;
    public static final int ROWS_PER_QUANTUM = 31;

    // Depending on concrete type, because we need getRowSetCoveringTimestampRange, and we control this object
    private static final TicketsCellEncodingStrategy ABORTED_TICKETS_ENCODING_STRATEGY =
            new TicketsCellEncodingStrategy(PARTITIONING_QUANTUM, ROWS_PER_QUANTUM);
    private static final byte[] MARKER_VALUE = PtBytes.EMPTY_BYTE_ARRAY;
    public static final int CELL_BATCH_HINT = 1000; // Values are small, so 1000 should be safe.

    private final KeyValueService keyValueService;

    public AbandonedTimestampStoreImpl(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
    }

    @Override
    public Set<Long> getAbandonedTimestampsInRange(long startInclusive, long endInclusive) {
        RowColumnRangeIterator iterator = keyValueService.getRowsColumnRange(
                TransactionConstants.KNOWN_ABANDONED_TIMESTAMPS_TABLE,
                ABORTED_TICKETS_ENCODING_STRATEGY
                        .getRowSetCoveringTimestampRange(startInclusive, endInclusive)
                        .collect(Collectors.toSet()),
                ABORTED_TICKETS_ENCODING_STRATEGY.getColumnRangeCoveringTimestampRange(startInclusive, endInclusive),
                CELL_BATCH_HINT,
                Long.MAX_VALUE);
        Set<Long> resultSet = new HashSet<>();
        iterator.forEachRemaining(entry -> {
            // There is an optimisation that relies on the ordering in which values are returned to perform fewer
            // checks.
            Cell cell = entry.getKey();
            long timestamp = ABORTED_TICKETS_ENCODING_STRATEGY.decodeCellAsStartTimestamp(cell);
            if (startInclusive <= timestamp && timestamp <= endInclusive) {
                resultSet.add(timestamp);
            }
        });
        return resultSet;
    }

    @Override
    public void markAbandoned(long timestampToAbort) {
        keyValueService.put(
                TransactionConstants.KNOWN_ABANDONED_TIMESTAMPS_TABLE,
                ImmutableMap.of(getTargetCell(timestampToAbort), MARKER_VALUE),
                AtlasDbConstants.TRANSACTION_TS);
    }

    @Override
    public void markAbandoned(Set<Long> timestampsToAbort) {
        Map<Cell, byte[]> updates = KeyedStream.of(timestampsToAbort)
                .mapKeys(AbandonedTimestampStoreImpl::getTargetCell)
                .map(_unused -> MARKER_VALUE)
                .collectToMap();
        keyValueService.put(
                TransactionConstants.KNOWN_ABANDONED_TIMESTAMPS_TABLE, updates, AtlasDbConstants.TRANSACTION_TS);
    }

    private static Cell getTargetCell(long timestamp) {
        return ABORTED_TICKETS_ENCODING_STRATEGY.encodeStartTimestampAsCell(timestamp);
    }
}
