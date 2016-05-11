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

import java.util.concurrent.Callable;

import com.palantir.atlasdb.cli.command.CleanTransactionRange;
import com.palantir.atlasdb.cli.command.KvsMigrationCommand;
import com.palantir.atlasdb.cli.command.SweepCommand;
import com.palantir.atlasdb.cli.command.FastForwardTimestamp;
import com.palantir.atlasdb.cli.command.TimestampCommand;

import io.airlift.airline.Cli;
import io.airlift.airline.Help;

public class AtlasCli {

    private static Cli<Callable> buildCli() {
        Cli.CliBuilder<Callable> builder = Cli.<Callable>builder("atlasdb")
                .withDescription("Perform common AtlasDB tasks")
                .withDefaultCommand(Help.class)
                .withCommands(
                        Help.class,
                        TimestampCommand.class,
                        CleanTransactionRange.class,
                        SweepCommand.class,
                        KvsMigrationCommand.class,
                        FastForwardTimestamp.class);
        return builder.build();
    }

    public static void main(String[] args) {
        Cli<Callable> parser = buildCli();
        try {
            Object ret = parser.parse(args).call();
            if (ret instanceof Integer) {
                System.exit((Integer) ret);
            }
            System.exit(0);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

}
