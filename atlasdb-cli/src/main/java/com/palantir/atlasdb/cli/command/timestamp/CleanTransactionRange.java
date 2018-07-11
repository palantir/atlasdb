/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.cli.command.timestamp;

import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.cli.output.OutputPrinter;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.base.ClosableIterator;
import com.palantir.logsafe.SafeArg;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

@Command(name = "clean-transactions", description = "Clean out the entries in a _transactions table for the "
        + "purpose of deleting potentially inconsistent transactions from an underlying database that lacks "
        + "PITR backup semantics.  Deletes all transactions with a commit timestamp greater than the timestamp "
        + "provided.")
public class CleanTransactionRange extends AbstractTimestampCommand {

    private static final OutputPrinter printer = new OutputPrinter(
            LoggerFactory.getLogger(CleanTransactionRange.class));

    @Option(name = {"-s", "--start-timestamp"},
            title = "TIMESTAMP",
            type = OptionType.GROUP,
            description = "The timestamp to begin scanning the _trasactions table at.")
    Long startTimestamp;

    @Option(name = {"--skip-start-timestamp-check"},
            title = "TIMESTAMP",
            type = OptionType.GROUP,
            description = "Skips the check start timestamp check")
    boolean skipStartTimestampCheck;


    @Override
    public boolean isOnlineRunSupported() {
        return false;
    }

    @Override
    protected boolean requireTimestamp() {
        return true;
    }

    @Override
    protected int executeTimestampCommand(AtlasDbServices services) {
        KeyValueService kvs = services.getKeyValueService();

        byte[] startBytes = TransactionConstants.getValueForTimestamp(startTimestamp);
        byte[] timestampBytes = TransactionConstants.getValueForTimestamp(timestamp);

        if (startBytes.length != timestampBytes.length && !skipStartTimestampCheck) {
            throw new RuntimeException(String.format(
                    "Start timestamp and timestamp to clean after need to have the same number of bytes! %s != %s", startBytes.length, timestampBytes.length));
        }

        ClosableIterator<RowResult<Value>> range = kvs.getRange(
                TransactionConstants.TRANSACTION_TABLE,
                startTimestamp == null
                        ? RangeRequest.all()
                        : RangeRequest.builder()
                                .startRowInclusive(startBytes)
                                .build(),
                Long.MAX_VALUE);

        Multimap<Cell, Long> toDelete = HashMultimap.create();

        long lastLoggedTsCountdown = 0;
        while (range.hasNext()) {
            RowResult<Value> row = range.next();
            byte[] rowName = row.getRowName();
            long startTs = TransactionConstants.getTimestampForValue(rowName);

            if (lastLoggedTsCountdown == 0) {
                printer.info("Currently at timestamp {}.", SafeArg.of("startTs", startTs));
                lastLoggedTsCountdown = 100000;
            }
            lastLoggedTsCountdown -= 1;

            Value value;
            try {
                value = row.getOnlyColumnValue();
            } catch (IllegalStateException e) {
                //this should never happen
                printer.error("Found a row in the transactions table that didn't have 1"
                        + " and only 1 column value: start={}", SafeArg.of("startTs", startTs));
                continue;
            }

            long commitTs = TransactionConstants.getTimestampForValue(value.getContents());
            if (commitTs <= timestamp) {
                continue; // this is a valid transaction
            }

            printer.info("Found and cleaning possibly inconsistent transaction: [start={}, commit={}]",
                    SafeArg.of("startTs", startTs), SafeArg.of("commitTs", commitTs));

            Cell key = Cell.create(rowName, TransactionConstants.COMMIT_TS_COLUMN);
            toDelete.put(key, value.getTimestamp());  //value.getTimestamp() should always be 0L
        }

        if (!toDelete.isEmpty()) {
            kvs.delete(TransactionConstants.TRANSACTION_TABLE, toDelete);
            printer.info("Delete completed.");
        } else {
            printer.info("Found no transactions after the given timestamp to delete.");
        }

        return 0;
    }
}
