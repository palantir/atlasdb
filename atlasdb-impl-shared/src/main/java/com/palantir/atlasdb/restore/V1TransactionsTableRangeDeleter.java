/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.restore;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.base.ClosableIterator;
import com.palantir.logsafe.SafeArg;
import com.palantir.timestamp.TimestampRange;
import java.util.OptionalLong;

public class V1TransactionsTableRangeDeleter implements TransactionTableRangeDeleter {
    private final KeyValueService kvs;
    private final OptionalLong startTimestamp;
    private final boolean skipStartTimestampCheck;
    private final OutputStateLogger stateLogger;

    public V1TransactionsTableRangeDeleter(
            KeyValueService kvs,
            OptionalLong startTimestamp,
            boolean skipStartTimestampCheck,
            OutputStateLogger stateLogger) {
        this.kvs = kvs;
        this.startTimestamp = startTimestamp;
        this.skipStartTimestampCheck = skipStartTimestampCheck;
        this.stateLogger = stateLogger;
    }

    @Override
    public void deleteRange(TimestampRange commitTimestampRange) {
        byte[] startBytes =
                TransactionConstants.getValueForTimestamp(startTimestamp.orElse(AtlasDbConstants.STARTING_TS));
        byte[] timestampBytes = TransactionConstants.getValueForTimestamp(commitTimestampRange.getLowerBound());

        if (startBytes.length != timestampBytes.length && !skipStartTimestampCheck) {
            throw new RuntimeException(String.format(
                    "Start timestamp and timestamp to clean after need to have the same number of bytes! %s != %s",
                    startBytes.length, timestampBytes.length));
        }

        ClosableIterator<RowResult<Value>> range = kvs.getRange(
                TransactionConstants.TRANSACTION_TABLE,
                !startTimestamp.isPresent()
                        ? RangeRequest.all()
                        : RangeRequest.builder().startRowInclusive(startBytes).build(),
                Long.MAX_VALUE);

        Multimap<Cell, Long> toDelete = HashMultimap.create();

        long lastLoggedTsCountdown = 0;
        while (range.hasNext()) {
            RowResult<Value> row = range.next();
            byte[] rowName = row.getRowName();
            long startTs = TransactionConstants.getTimestampForValue(rowName);

            if (lastLoggedTsCountdown == 0) {
                stateLogger.info("Currently at timestamp {}.", SafeArg.of("startTs", startTs));
                lastLoggedTsCountdown = 100000;
            }
            lastLoggedTsCountdown -= 1;

            Value value;
            try {
                value = row.getOnlyColumnValue();
            } catch (IllegalStateException e) {
                // this should never happen
                stateLogger.error(
                        "Found a row in the transactions table that didn't have 1"
                                + " and only 1 column value: start={}",
                        SafeArg.of("startTs", startTs));
                continue;
            }

            long commitTs = TransactionConstants.getTimestampForValue(value.getContents());
            if (!commitTimestampRange.contains(commitTs)) {
                continue; // this is not a transaction we are targeting
            }

            stateLogger.info(
                    "Found and cleaning possibly inconsistent transaction: [start={}, commit={}]",
                    SafeArg.of("startTs", startTs),
                    SafeArg.of("commitTs", commitTs));

            Cell key = Cell.create(rowName, TransactionConstants.COMMIT_TS_COLUMN);
            toDelete.put(key, value.getTimestamp()); // value.getTimestamp() should always be 0L
        }

        if (!toDelete.isEmpty()) {
            kvs.delete(TransactionConstants.TRANSACTION_TABLE, toDelete);
            stateLogger.info("Delete completed.");
        } else {
            stateLogger.info("Found no transactions after the given timestamp to delete.");
        }
    }
}
