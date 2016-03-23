/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.cli.command;

import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.palantir.atlasdb.cli.services.AtlasDbServices;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.base.ClosableIterator;
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.TimestampService;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

@Command(name = "cleanTransactionRange", description = "Clean a recently restored backup of a transaction table from an underlying database that lacks PITR backup semantics.")
public class CleanTransactionRange extends SingleBackendCommand {

    @Option(name = {"-s", "--start"},
            title = "START_TIMESTAMP",
            description = "Start timestamp; will cleanup transactions after this point",
            required = true)
    long startTimestampExclusive;

    @Option(name = {"-d", "--delete"},
            description = "Actually delete the range from the transaction table; as opposed to just marking them as cleaned")
    boolean delete;

    final byte[] cleaned = TransactionConstants.getValueForTimestamp(TransactionConstants.CLEANED_COMMIT_TS);

    @Override
    public int execute(AtlasDbServices services) {
        long immutable = services.getTransactionManager().getImmutableTimestamp();
        TimestampService ts = services.getTimestampService();
        if(!isValid(immutable, ts)) {
            return 1;
        }

        PersistentTimestampService pts = (PersistentTimestampService) ts;
        KeyValueService kvs = services.getKeyValueService();

        byte[] startRowInclusive = RangeRequests.nextLexicographicName(TransactionConstants.getValueForTimestamp(startTimestampExclusive));
        ClosableIterator<RowResult<Value>> range = kvs.getRange(
                TransactionConstants.TRANSACTION_TABLE,
                RangeRequest.builder()
                .startRowInclusive(startRowInclusive)
                .build(),
                Long.MAX_VALUE);

        Map<Cell, byte[]> toClean = Maps.newHashMap();

        long maxTimestamp = startTimestampExclusive;
        while (range.hasNext()) {
            RowResult<Value> row = range.next();
            long startResult = TransactionConstants.getTimestampForValue(row.getRowName());
            maxTimestamp = Math.max(maxTimestamp, startResult);
            long endResult = 0L;
            if (row.getOnlyColumnValue() != null) {
                endResult = TransactionConstants.getTimestampForValue(row.getOnlyColumnValue().getContents());
                maxTimestamp = Math.max(maxTimestamp, endResult);
            } else {
                //this should never happen
                System.out.printf("Error: Found a row in the transactions table that didn't have 1 and only 1 column value: start=%d", startResult);
            }

            System.out.printf("Found and cleaning possibly inconsistent transaction: [start=%d, commit=%d]", startResult, endResult);

            for (Cell cell : row.getCellSet()) {
                toClean.put(cell, cleaned);
            }
        }

        if (!toClean.isEmpty()) {
            if (delete) {
                Multimap<Cell, byte[]> toCleanMulti = Multimaps.forMap(toClean);
                Multimap<Cell, Long> toDelete = Multimaps.transformValues(toCleanMulti, new Function<byte[], Long>() {
                    public Long apply(byte[] in) {
                        return TransactionConstants.getTimestampForValue(in);
                    }
                });
                kvs.delete(TransactionConstants.TRANSACTION_TABLE, toDelete);
                System.out.println("\nDelete completed.");
            } else {
                kvs.putUnlessExists(TransactionConstants.TRANSACTION_TABLE, toClean);
                System.out.println("\nClean completed.");
            }
        } else {
            System.out.println("Found no transactions inside the given range to clean up or delete.");
        }

        pts.fastForwardTimestamp(maxTimestamp);
        System.out.printf("Timestamp succesfully forwarded past all cleaned/deleted transactions to {}", maxTimestamp);

        return 0;
    }

    private boolean isValid(long immutableTimestamp, TimestampService ts) {
        boolean isValid = true;

        if (immutableTimestamp <= startTimestampExclusive) {
            System.err.println("Error: There are no commited transactions in this range");
            isValid &= false;
        }

        if (!(ts instanceof PersistentTimestampService)) {
            System.err.printf("Error: Restoring timestamp service must be of type {}, but yours is {}",
                    PersistentTimestampService.class.toString(), ts.getClass().toString());
            isValid &= false;
        }

        return isValid;
    }

}
