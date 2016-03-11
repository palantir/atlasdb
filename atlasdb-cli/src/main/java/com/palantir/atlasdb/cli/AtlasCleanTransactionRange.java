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

package com.palantir.atlasdb.cli;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import com.google.common.base.Functions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.palantir.atlasdb.cli.api.AtlasDbServices;
import com.palantir.atlasdb.cli.api.SingleBackendCommand;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.base.ClosableIterator;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

@Command(name = "cleanTransactionRange", description = "Clean a recently restored backup of a transaction table from an underlying database that lacks PITR backup semantics.")
public class AtlasCleanTransactionRange extends SingleBackendCommand {
    @Option(name = {"-s", "--start"},
            description = "Start timestamp; will cleanup transactions after this point",
            required = true)
    long startTimestamp;

    @Option(name = {"-f", "--force"},
            description = "Disable interaction and always accept prompts.")
    boolean force;


    @Override
    protected int execute(AtlasDbServices services) {
        KeyValueService kvs = services.getKeyValueService();


        ClosableIterator<RowResult<Value>> range = kvs.getRange(
                TransactionConstants.TRANSACTION_TABLE,
                RangeRequest.builder()
                        .startRowInclusive(TransactionConstants.getValueForTimestamp(startTimestamp))
                        .build(),
                Long.MAX_VALUE); // currently we store this list of transactions (and indeed, anything stored with putUnlessExists) at TS=0

        Multimap<Cell, Long> toDelete = HashMultimap.create();

        while (range.hasNext()) {
            RowResult<Value> row = range.next();
            long startResult = TransactionConstants.getTimestampForValue(row.getRowName());
            long endResult = 0L;
            if (row.getOnlyColumnValue() != null) {
                endResult = TransactionConstants.getTimestampForValue(row.getOnlyColumnValue().getContents());
            }

            System.out.println(String.format("Found possibly inconsistent transaction: [start=%d, commit=%d]", startResult, endResult));

            for (Cell cell : row.getCellSet()) {
                toDelete.put(cell, 0L);
            }
        }

        if (!toDelete.isEmpty()) {
            if (!force) {
                System.out.print("Are you sure you wish to delete these transactions from the database? (y/n): ");
                if (!new Scanner(System.in).next().equalsIgnoreCase("y")) {
                    System.out.println("\nExiting without performing deletes.");
                    return 0;
                }
            }
            kvs.delete(TransactionConstants.TRANSACTION_TABLE, toDelete);
            System.out.println("\nDelete completed.");
        } else {
            System.out.println("Found no transactions inside the given range to clean up. No deletions necessary.");
        }

        return 0;
    }
}
