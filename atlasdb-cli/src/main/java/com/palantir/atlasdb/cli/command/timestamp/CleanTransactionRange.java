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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cli.output.OutputPrinter;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.base.ClosableIterator;
import com.palantir.logsafe.SafeArg;

import io.airlift.airline.Command;

@Command(name = "clean-transactions", description = "Clean out the entries in a _transactions table for the "
        + "purpose of deleting potentially inconsistent transactions from an underlying database that lacks "
        + "PITR backup semantics.  Deletes all transactions with a commit timestamp greater than the timestamp "
        + "provided.")
public class CleanTransactionRange extends AbstractTimestampCommand {

    private static final OutputPrinter printer = new OutputPrinter(
            LoggerFactory.getLogger(CleanTransactionRange.class));

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

        ClosableIterator<RowResult<Value>> range = kvs.getRange(
                TransactionConstants.TRANSACTION_TABLE,
                RangeRequest.all(),
                Long.MAX_VALUE);

        Map<Cell, byte[]> txTableValuesToWrite =  new HashMap<>();
        while (range.hasNext()) {
            RowResult<Value> row = range.next();
            byte[] rowName = row.getRowName();
            long startTs = TransactionConstants.getTimestampForValue(rowName);

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
            byte[] valueForRollbackTimestamp =
                    TransactionConstants.getValueForTimestamp(TransactionConstants.FAILED_COMMIT_TS);

            txTableValuesToWrite.put(key, valueForRollbackTimestamp);
        }

        if (!txTableValuesToWrite.isEmpty()) {
            Map<TableReference, Map<Cell, byte[]>> valuesToPut = new HashMap<>();
            valuesToPut.put(TransactionConstants.TRANSACTION_TABLE, txTableValuesToWrite);
            kvs.multiPut(valuesToPut, AtlasDbConstants.TRANSACTION_TS);
            printer.info("Completed rollback of transactions after the given timestamp.");
        } else {
            printer.info("Found no transactions after the given timestamp to rollback.");
        }

        return 0;
    }
}
