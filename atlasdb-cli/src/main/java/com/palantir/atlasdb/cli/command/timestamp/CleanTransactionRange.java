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
package com.palantir.atlasdb.cli.command.timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.base.ClosableIterator;

import io.airlift.airline.Command;

@Command(name = "clean-transactions", description = "Clean a recently restored backup of a _transactions table "
        + "from an underlying database that lacks PITR backup semantics.  Deletes all transactions with a "
        + "commit timestamp greater than the timestamp provided.")
public class CleanTransactionRange extends AbstractTimestampCommand {

    private static final Logger log = LoggerFactory.getLogger(CleanTransactionRange.class);

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

        byte[] startRowInclusive = TransactionConstants.getValueForTimestamp(timestamp);
        ClosableIterator<RowResult<Value>> range = kvs.getRange(
                TransactionConstants.TRANSACTION_TABLE,
                RangeRequest.builder()
                        .startRowInclusive(startRowInclusive)
                        .build(),
                Long.MAX_VALUE);

        Multimap<Cell, Long> toDelete = HashMultimap.create();
        while (range.hasNext()) {
            RowResult<Value> row = range.next();
            byte[] rowName = row.getRowName();
            long startTs = TransactionConstants.getTimestampForValue(rowName);

            Value value;
            try {
                value = row.getOnlyColumnValue();
            } catch (IllegalStateException e){
                //this should never happen
                log.error("Found a row in the transactions table that didn't have 1 and only 1 column value: start={}", startTs);
                continue;
            }

            long commitTs = TransactionConstants.getTimestampForValue(value.getContents());
            if (commitTs <= timestamp) {
                continue; // this is a valid transaction
            }

            log.info("Found and cleaning possibly inconsistent transaction: [start={}, commit={}]", startTs, commitTs);

            Cell key = Cell.create(rowName, TransactionConstants.COMMIT_TS_COLUMN);
            toDelete.put(key, value.getTimestamp());  //value.getTimestamp() should always be 0L
        }

        if (!toDelete.isEmpty()) {
            kvs.delete(TransactionConstants.TRANSACTION_TABLE, toDelete);
            log.info("Delete completed.");
        } else {
            log.info("Found no transactions after the given timestamp to delete.");
        }

        return 0;
    }
}
