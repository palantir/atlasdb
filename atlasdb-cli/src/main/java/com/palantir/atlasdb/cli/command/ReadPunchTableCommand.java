/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import java.util.Date;

import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.cleaner.KeyValueServicePuncherStore;
import com.palantir.atlasdb.cleaner.PuncherStore;
import com.palantir.atlasdb.cli.command.timestamp.AbstractTimestampCommand;
import com.palantir.atlasdb.cli.output.OutputPrinter;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.logsafe.SafeArg;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

@Command(name = "read-punch-table", description = "Given an epoch time in millis, read the timestamp recorded"
        + " just before it in the punch table.")
public class ReadPunchTableCommand extends SingleBackendCommand {
    private static final OutputPrinter printer = new OutputPrinter(
            LoggerFactory.getLogger(AbstractTimestampCommand.class));

    @Option(name = {"-e", "--epoch"},
            title = "EPOCH TIME",
            type = OptionType.COMMAND,
            description = "The epoch time to read the first value from. This should be epoch time in millis.")
    Long epochTime;

    @Override
    public boolean isOnlineRunSupported() {
        return true;
    }

    @Override
    public int execute(AtlasDbServices services) {
        if (epochTime == null) {
            throw new IllegalArgumentException("Required option '-e' is missing");
        }

        Date date = new Date(epochTime);
        printer.info("Input {} in epoch millis is {}",
                SafeArg.of("epochMillis", epochTime),
                SafeArg.of("date", date));

        KeyValueService keyValueService = services.getKeyValueService();
        PuncherStore puncherStore = KeyValueServicePuncherStore.create(keyValueService, false);
        Long value = puncherStore.get(epochTime);
        printer.info("The first timestamp before {} is {}",
                SafeArg.of("date", date),
                SafeArg.of("timestamp", value));
        return 0;
    }
}
