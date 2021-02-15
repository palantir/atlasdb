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
package com.palantir.atlasdb.cli.command;

import com.palantir.atlasdb.cleaner.KeyValueServicePuncherStore;
import com.palantir.atlasdb.cleaner.PuncherStore;
import com.palantir.atlasdb.cli.command.timestamp.AbstractTimestampCommand;
import com.palantir.atlasdb.cli.output.OutputPrinter;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.slf4j.LoggerFactory;

@Command(
        name = "read-punch-table",
        description =
                "Given an epoch time in millis, read the timestamp recorded" + " just before it in the punch table.")
public class ReadPunchTableCommand extends SingleBackendCommand {
    private static final OutputPrinter printer =
            new OutputPrinter(LoggerFactory.getLogger(AbstractTimestampCommand.class));

    @Option(
            name = {"-e", "--epoch"},
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
            throw new SafeIllegalArgumentException("Required option '-e' is missing");
        }
        if (epochTime < 0) {
            throw new SafeIllegalArgumentException(
                    "Option '-e' should be a positive long, as epoch time" + " is never negative.");
        }

        Instant epochTimeInstant = Instant.ofEpochSecond(epochTime);
        ZonedDateTime date = ZonedDateTime.ofInstant(epochTimeInstant, ZoneOffset.UTC);
        printer.info(
                "Input {} in epoch millis is {}",
                SafeArg.of("epochMillis", epochTime),
                SafeArg.of("date", date.toString()));

        KeyValueService keyValueService = services.getKeyValueService();
        PuncherStore puncherStore = KeyValueServicePuncherStore.create(keyValueService, false);
        Long value = puncherStore.get(epochTime);
        printer.info(
                "The first timestamp before {} is {}",
                SafeArg.of("date", date.toString()),
                SafeArg.of("timestamp", value));
        return 0;
    }
}
