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
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.logsafe.SafeArg;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;
import io.airlift.airline.Command;
import org.slf4j.LoggerFactory;

@Command(
        name = "fast-forward",
        description = "Fast forward the stored upper limit of a persistent timestamp service to the specified"
                + " timestamp. Is used in the restore process to ensure that all future timestamps used are explicity"
                + " greater than any that may have been used to write data to the KVS before backing up the underlying"
                + " storage.")
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
        printer.warn(
                "This CLI has been deprecated. Please use the timestamp-management/fast-forward endpoint instead.");

        TimestampService ts = services.getManagedTimestampService();
        if (!(ts instanceof TimestampManagementService)) {
            printer.error(
                    "Timestamp service must be of type {}, but yours is {}.  Exiting.",
                    SafeArg.of("expected type", TimestampManagementService.class.toString()),
                    SafeArg.of("found type", ts.getClass().toString()));
            return 1;
        }
        TimestampManagementService tms = (TimestampManagementService) ts;

        tms.fastForwardTimestamp(timestamp);
        printer.info("Timestamp successfully fast-forwarded to {}", SafeArg.of("timestamp", timestamp));
        return 0;
    }
}
