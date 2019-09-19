/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.cli.command.timestamp;

import com.palantir.atlasdb.cli.output.OutputPrinter;
import com.palantir.atlasdb.restore.V1TransactionsTableRangeDeleter;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.timestamp.TimestampRange;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import java.util.OptionalLong;
import org.slf4j.LoggerFactory;

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
        new V1TransactionsTableRangeDeleter(
                services.getKeyValueService(),
                startTimestamp == null ? OptionalLong.empty() : OptionalLong.of(startTimestamp),
                skipStartTimestampCheck,
                printer).deleteRange(TimestampRange.createInclusiveRange(timestamp, Long.MAX_VALUE));
        return 0;
    }
}
