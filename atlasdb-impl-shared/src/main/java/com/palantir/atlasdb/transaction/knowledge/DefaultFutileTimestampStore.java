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
import com.palantir.atlasdb.transaction.encoding.TicketsCellEncodingStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import java.util.Set;
import java.util.stream.Stream;

public class DefaultFutileTimestampStore implements FutileTimestampStore {
    // DO NOT change the following without a migration of the known aborted timestamps table!
    // These values were chosen on the basis that unlike in transactions2, values provided are markers and so we can
    // store more values per row (because we don't have a commit timestamp that we also need to store).
    public static final long PARTITIONING_QUANTUM = 50_000_000;
    public static final int ROWS_PER_QUANTUM = 31;

    // Depending on concrete type, because we need getRowSetCoveringTimestampRange, and we control this object
    private static final TicketsCellEncodingStrategy ABORTED_TICKETS_ENCODING_STRATEGY =
            new TicketsCellEncodingStrategy(PARTITIONING_QUANTUM, ROWS_PER_QUANTUM);
    private static final byte[] MARKER_VALUE = PtBytes.EMPTY_BYTE_ARRAY;

    private final KeyValueService keyValueService;

    public DefaultFutileTimestampStore(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
    }

    @Override
    public Set<Long> getFutileTimestampsInRange(long startInclusive, long endInclusive) {
        Stream<byte[]> rows =
                ABORTED_TICKETS_ENCODING_STRATEGY.getRowSetCoveringTimestampRange(startInclusive, endInclusive);
        return null;
    }

    @Override
    public void markFutile(long timestampToAbort) {
        keyValueService.put(
                TransactionConstants.KNOWN_ABORTED_TIMESTAMPS_TABLE,
                ImmutableMap.of(getTargetCell(timestampToAbort), MARKER_VALUE),
                AtlasDbConstants.TRANSACTION_TS);
    }

    private static Cell getTargetCell(long timestamp) {
        return ABORTED_TICKETS_ENCODING_STRATEGY.encodeStartTimestampAsCell(timestamp);
    }
}
