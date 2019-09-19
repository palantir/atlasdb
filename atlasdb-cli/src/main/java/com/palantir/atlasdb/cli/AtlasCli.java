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
package com.palantir.atlasdb.cli;

import com.palantir.atlasdb.cli.command.KvsMigrationCommand;
import com.palantir.atlasdb.cli.command.ReadPunchTableCommand;
import com.palantir.atlasdb.cli.command.ScrubQueueMigrationCommand;
import com.palantir.atlasdb.cli.command.SweepCommand;
import com.palantir.atlasdb.cli.command.timestamp.CleanTransactionRange;
import com.palantir.atlasdb.cli.command.timestamp.FastForwardTimestamp;
import com.palantir.atlasdb.cli.command.timestamp.FetchTimestamp;
import io.airlift.airline.Cli;
import io.airlift.airline.Help;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AtlasCli {

    private static final Logger log = LoggerFactory.getLogger(AtlasCli.class);

    private AtlasCli() {}

    public static Cli<Callable<?>> buildCli() {
        Cli.CliBuilder<Callable<?>> builder = Cli.<Callable<?>>builder("atlasdb")
                .withDescription("Perform common AtlasDB tasks")
                .withDefaultCommand(Help.class)
                .withCommand(Help.class)
                .withCommand(SweepCommand.class)
                .withCommand(KvsMigrationCommand.class)
                .withCommand(ScrubQueueMigrationCommand.class)
                .withCommand(ReadPunchTableCommand.class);

        builder.withGroup("timestamp")
                .withDescription("Timestamp-centric commands")
                .withDefaultCommand(Help.class)
                .withCommand(FetchTimestamp.class)
                .withCommand(CleanTransactionRange.class)
                .withCommand(FastForwardTimestamp.class);

        return builder.build();
    }

    public static void main(String[] args) {
        Cli<Callable<?>> parser = buildCli();
        try {
            Object ret = parser.parse(args).call();
            if (ret instanceof Integer) {
                System.exit((Integer) ret);
            }
            System.exit(0);
        } catch (Exception e) {
            log.error("Fatal exception thrown during cli command execution.  Exiting.", e);
            System.exit(1);
        }
    }

}
