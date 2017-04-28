/*
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

import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.cli.output.OutputPrinter;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;

import io.airlift.airline.Command;

@Command(name = "fast-forward", description = "Fast forward the stored upper limit of a persistent timestamp"
        + " service to the specified timestamp."
        + " Is used in the restore process to ensure that all future timestamps used are explicity greater than any"
        + " that may have been used to write data to the KVS before backing up the underlying storage.")
public class FastForwardTimestamp extends AbstractTimestampCommand {
    private static final OutputPrinter printer = new OutputPrinter(LoggerFactory.getLogger(FastForwardTimestamp.class));

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
        TimestampService ts = services.getTimestampService();
        if (!(ts instanceof TimestampManagementService)) {
            printer.error("Timestamp service must be of type {}, but yours is {}.  Exiting.",
                    TimestampManagementService.class.toString(), ts.getClass().toString());
            return 1;
        }
        TimestampManagementService tms = (TimestampManagementService) ts;

        tms.fastForwardTimestamp(timestamp);
        printer.info("Timestamp successfully fast-forwarded to {}", timestamp);
        return 0;
    }
}
