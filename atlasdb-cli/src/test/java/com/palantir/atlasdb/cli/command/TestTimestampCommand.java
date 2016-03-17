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
package com.palantir.atlasdb.cli.command;

import java.util.Scanner;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.atlasdb.cli.SingleBackendCliTests;
import com.palantir.atlasdb.cli.services.AtlasDbServices;
import com.palantir.atlasdb.cli.services.AtlasDbServicesModuleFactory;
import com.palantir.atlasdb.cli.services.ServicesConfigModule;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.timestamp.TimestampService;

import io.airlift.airline.Cli;

public class TestTimestampCommand {

    private static String configPath;
    private static Cli<SingleBackendCommand> cli;
    private static LockDescriptor lock;
    private static LockClient client;
    private static AtlasDbServicesModuleFactory moduleFactory;

    @BeforeClass
    public static void setup() throws Exception {
        configPath = SingleBackendCliTests.getConfigPath(SingleBackendCliTests.SIMPLE_ROCKSDB_CONFIG_FILENAME);
        cli = SingleBackendCliTests.build(TimestampCommand.class);
        lock = StringLockDescriptor.of("lock");
        client = LockClient.of("test lock client");
        moduleFactory = config -> new ServicesConfigModule(config) {};
    }

    @Test
    public void testBasicInvariants() throws Exception {
        SingleBackendCommand cmd = cli.parse("timestamp", "-c", configPath, "-f", "-i");
        try (AtlasDbServices services = cmd.connect(moduleFactory)) {
            RemoteLockService rls = services.getLockSerivce();
            TimestampService tss = services.getTimestampService();

            long lockedTs = tss.getFreshTimestamp();
            LockRequest request = LockRequest.builder(ImmutableSortedMap.of(
                    lock, LockMode.WRITE))
                    .withLockedInVersionId(lockedTs).doNotBlock().build();
            LockRefreshToken token = rls.lockWithClient(client.getClientId(), request);

            Scanner scanner = new Scanner(SingleBackendCliTests.captureStdOut(() -> cmd.execute(services), true));
            final long fresh = Long.parseLong(scanner.findInLine("\\d+"));
            final long immutable = Long.parseLong(scanner.findInLine("\\d+"));
            Preconditions.checkArgument(immutable <= lockedTs);
            Preconditions.checkArgument(fresh > lockedTs);
            Preconditions.checkArgument(fresh < tss.getFreshTimestamp());

            rls.unlock(token);

            scanner = new Scanner(SingleBackendCliTests.captureStdOut(() -> cmd.execute(services), true));
            final long newFresh = Long.parseLong(scanner.findInLine("\\d+"));
            final long newImmutable = Long.parseLong(scanner.findInLine("\\d+"));
            Preconditions.checkArgument(newFresh > fresh);
            Preconditions.checkArgument(newImmutable > lockedTs);
        }
    }

}
