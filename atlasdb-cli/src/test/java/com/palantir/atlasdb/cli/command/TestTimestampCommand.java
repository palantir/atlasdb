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
import com.palantir.atlasdb.cli.runner.RocksDbTestRunner;
import com.palantir.atlasdb.cli.runner.SingleBackendCliTestRunner;
import com.palantir.atlasdb.cli.services.AtlasDbServicesFactory;
import com.palantir.atlasdb.cli.services.DaggerTestAtlasDbServices;
import com.palantir.atlasdb.cli.services.ServicesConfigModule;
import com.palantir.atlasdb.cli.services.TestAtlasDbServices;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.timestamp.TimestampService;

public class TestTimestampCommand {

    private static LockDescriptor lock;
    private static AtlasDbServicesFactory moduleFactory;

    @BeforeClass
    public static void setup() throws Exception {
        lock = StringLockDescriptor.of("lock");
        moduleFactory = new AtlasDbServicesFactory() {
            @Override
            public TestAtlasDbServices connect(ServicesConfigModule servicesConfigModule) {
                return DaggerTestAtlasDbServices.builder()
                        .servicesConfigModule(servicesConfigModule)
                        .build();
            }
        };
    }

    private RocksDbTestRunner makeRunner(String... args) {
        return new RocksDbTestRunner(TimestampCommand.class, args);
    }

    @Test
    public void testBasicInvariants() throws Exception {
        try (SingleBackendCliTestRunner runner = makeRunner("-f", "-i")) {
            TestAtlasDbServices services = runner.connect(moduleFactory);
            RemoteLockService rls = services.getLockSerivce();
            TimestampService tss = services.getTimestampService();
            LockClient client = services.getTestLockClient();

            long lockedTs = tss.getFreshTimestamp();
            LockRequest request = LockRequest.builder(ImmutableSortedMap.of(
                    lock, LockMode.WRITE))
                    .withLockedInVersionId(lockedTs).doNotBlock().build();
            LockRefreshToken token = rls.lock(client.getClientId(), request);

            runner.run();

            Scanner scanner = new Scanner(runner.run());
            final long fresh = Long.parseLong(scanner.findInLine("\\d+"));
            final long immutable = Long.parseLong(scanner.findInLine("\\d+"));
            Preconditions.checkArgument(immutable <= lockedTs);
            Preconditions.checkArgument(fresh > lockedTs);
            Preconditions.checkArgument(fresh < tss.getFreshTimestamp());

            rls.unlock(token);

            scanner = new Scanner(runner.run());
            final long newFresh = Long.parseLong(scanner.findInLine("\\d+"));
            final long newImmutable = Long.parseLong(scanner.findInLine("\\d+"));
            Preconditions.checkArgument(newFresh > fresh);
            Preconditions.checkArgument(newImmutable > lockedTs);
        }
    }

}
