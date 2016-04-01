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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
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
            description = "Start timestamp; will cleanup transactions after (but not including) this timestamp",
            required = true)
    long startTimestampExclusive;

    //clean has yet to be verified; always delete
    //@Option(name = {"-d", "--delete"},
    //        description = "Actually delete the range from the transaction table; as opposed to just marking them as cleaned")
    boolean delete = true;

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
        Multimap<Cell, Long> toDelete = HashMultimap.create();

        long maxTimestamp = startTimestampExclusive;
        while (range.hasNext()) {
            RowResult<Value> row = range.next();
            byte[] rowName = row.getRowName();
            long startResult = TransactionConstants.getTimestampForValue(rowName);
            maxTimestamp = Math.max(maxTimestamp, startResult);
            
            Value value;
            try {
                value = row.getOnlyColumnValue();
            } catch (IllegalStateException e){
                //this should never happen
                System.out.printf("Error: Found a row in the transactions table that didn't have 1 and only 1 column value: start=%d\n", startResult);
                continue;
            }

            long endResult = TransactionConstants.getTimestampForValue(value.getContents());
            maxTimestamp = Math.max(maxTimestamp, endResult);
            System.out.printf("Found and cleaning possibly inconsistent transaction: [start=%d, commit=%d]\n", startResult, endResult);

            Cell key = Cell.create(rowName, TransactionConstants.COMMIT_TS_COLUMN);
            toClean.put(key, cleaned);
            toDelete.put(key, value.getTimestamp());  //value.getTimestamp() should always be 0L but this is safer
        }

        if (!toClean.isEmpty()) {
            if (delete) {
                kvs.delete(TransactionConstants.TRANSACTION_TABLE, toDelete);
                System.out.println("Delete completed.");
            } else {
                kvs.putUnlessExists(TransactionConstants.TRANSACTION_TABLE, toClean);
                System.out.println("Clean completed.");
            }
            
            pts.fastForwardTimestamp(maxTimestamp);
            System.out.printf("Timestamp succesfully forwarded past all cleaned/deleted transactions to %d\n", maxTimestamp);
        } else {
            System.out.println("Found no transactions inside the given range to clean up or delete.");
        }

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
