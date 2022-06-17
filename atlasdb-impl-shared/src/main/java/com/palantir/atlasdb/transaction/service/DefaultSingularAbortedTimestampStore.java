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

package com.palantir.atlasdb.transaction.service;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.encoding.AbortedTimestampTicketsEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.CellEncodingStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;

public final class DefaultSingularAbortedTimestampStore implements SingularAbortedTimestampStore {
    private static final CellEncodingStrategy ENCODING_STRATEGY = AbortedTimestampTicketsEncodingStrategy.INSTANCE;
    private static final byte[] VALUE = PtBytes.EMPTY_BYTE_ARRAY;

    private final KeyValueService keyValueService;

    public DefaultSingularAbortedTimestampStore(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
    }

    @Override
    public boolean isTimestampAborted(long timestamp) {
        Cell targetCell = getTargetCell(timestamp);
        return !keyValueService
                .get(TransactionConstants.KNOWN_ABORTED_TIMESTAMPS, ImmutableMap.of(targetCell, Long.MAX_VALUE))
                .isEmpty();
    }

    @Override
    public void abortTimestamp(long timestampToAbort) {
        keyValueService.put(
                TransactionConstants.KNOWN_ABORTED_TIMESTAMPS,
                ImmutableMap.of(getTargetCell(timestampToAbort), VALUE),
                AtlasDbConstants.TRANSACTION_TS);
    }

    private static Cell getTargetCell(long timestamp) {
        return ENCODING_STRATEGY.encodeStartTimestampAsCell(timestamp);
    }
}
