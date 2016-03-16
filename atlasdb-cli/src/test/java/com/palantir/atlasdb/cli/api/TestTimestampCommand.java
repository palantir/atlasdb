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
package com.palantir.atlasdb.cli.api;

import java.util.Scanner;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.cli.TimestampCommand;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.impl.RawTransaction;
import com.palantir.lock.LockRefreshToken;

import io.airlift.airline.Cli;

public class TestTimestampCommand {

    private static String configPath;
    private static Cli<SingleBackendCommand> cli;

    @BeforeClass
    public static void setup() throws Exception {
        configPath = SingleBackendCliTests.getConfigPath(SingleBackendCliTests.SIMPLE_ROCKSDB_CONFIG_FILENAME);
        cli = SingleBackendCliTests.build(TimestampCommand.class);
    }

    @Test
    public void testBasicInvariants() throws Exception {
        SingleBackendCommand cmd = cli.parse("timestamp", "-c", configPath, "-f", "-i");
        try (OldAtlasDbServices services = cmd.connect()) {
            long initTimestamp = services.getTimestampService().getFreshTimestamp();
            RawTransaction tx = services.getTransactionManager().setupRunTaskWithLocksThrowOnConflict(ImmutableList.<LockRefreshToken>of());
            long afterLockTimestamp = services.getTimestampService().getFreshTimestamp();

            Scanner scanner = new Scanner(SingleBackendCliTests.captureStdOut(() -> cmd.execute(services), true));
            final long fresh = Long.parseLong(scanner.findInLine("\\d+"));
            final long immutable = Long.parseLong(scanner.findInLine("\\d+"));

            Preconditions.checkArgument(fresh > initTimestamp);
            Preconditions.checkArgument(fresh > afterLockTimestamp);
            Preconditions.checkArgument(immutable > initTimestamp);
            Preconditions.checkArgument(immutable < afterLockTimestamp);
            services.getTransactionManager().finishRunTaskWithLockThrowOnConflict(tx, (TransactionTask<Void, Exception>) t -> null);

            scanner = new Scanner(SingleBackendCliTests.captureStdOut(() -> cmd.execute(services), true));
            final long newFresh = Long.parseLong(scanner.findInLine("\\d+"));
            final long newImmutable = Long.parseLong(scanner.findInLine("\\d+"));
            Preconditions.checkArgument(newFresh > fresh);
            Preconditions.checkArgument(newImmutable > afterLockTimestamp);
        }
    }

}
