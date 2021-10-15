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

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.cli.command.timestamp.FastForwardTimestamp;
import com.palantir.atlasdb.cli.command.timestamp.FetchTimestamp;
import com.palantir.atlasdb.cli.runner.InMemoryTestRunner;
import com.palantir.atlasdb.factory.InMemoryLockAndTimestampServiceFactory;
import com.palantir.atlasdb.factory.LockAndTimestampServices;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.services.AtlasDbServicesFactory;
import com.palantir.atlasdb.services.LockAndTimestampModule;
import com.palantir.atlasdb.services.ServicesConfig;
import com.palantir.atlasdb.services.ServicesConfigModule;
import com.palantir.atlasdb.services.test.DaggerTestAtlasDbServices;
import com.palantir.atlasdb.services.test.TestAtlasDbServices;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.timelock.paxos.InMemoryTimeLockRule;
import com.palantir.timelock.paxos.InMemoryTimelockServices;
import io.airlift.airline.Command;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class TestFastForwardTimestampCommand {
    private static final String TIMESTAMP_GROUP = "timestamp";
    private static final String FETCH_COMMAND =
            FetchTimestamp.class.getAnnotation(Command.class).name();
    private static final String FAST_FORWARD_COMMAND =
            FastForwardTimestamp.class.getAnnotation(Command.class).name();
    private static final long POSITIVE_OFFSET = 777L;
    private static final long NEGATIVE_OFFSET = -1 * POSITIVE_OFFSET;

    private AtlasDbServicesFactory moduleFactory;

    @ClassRule
    public static InMemoryTimeLockRule inMemoryTimeLockRule = new InMemoryTimeLockRule();

    @Before
    public void setUp() {
        moduleFactory = createModuleFactory(inMemoryTimeLockRule.get());
    }

    @Test
    public void canFastForwardTimestamp() throws Exception {
        long currentTimestamp = fetchCurrentTimestamp();
        checkFastForward(currentTimestamp + POSITIVE_OFFSET, currentTimestamp + POSITIVE_OFFSET);
    }

    @Test
    public void fastForwardToThePastIsANoOp() throws Exception {
        long currentTimestamp = fetchCurrentTimestamp();
        checkFastForward(currentTimestamp + NEGATIVE_OFFSET, currentTimestamp);
    }

    @Test
    public void fastForwardToMinValueIsANoOp() throws Exception {
        long currentTimestamp = fetchCurrentTimestamp();
        checkFastForward(Long.MIN_VALUE + 1, currentTimestamp);
    }

    private long fetchCurrentTimestamp() throws Exception {
        InMemoryTestRunner fetchRunner = new InMemoryTestRunner(FetchTimestamp.class, TIMESTAMP_GROUP, FETCH_COMMAND);
        AtlasDbServices services = fetchRunner.connect(moduleFactory);
        return services.getManagedTimestampService().getFreshTimestamp();
    }

    private void checkFastForward(long target, long expected) throws Exception {
        InMemoryTestRunner runner = makeRunnerWithTargetTimestamp(target);
        AtlasDbServices atlasDbServices = runner.connect(moduleFactory);

        String response = runner.run();
        // Unintuitive, but is consistent with existing CLI behaviour.
        assertThat(response).contains("Timestamp successfully fast-forwarded to " + target);
        assertThat(atlasDbServices.getManagedTimestampService().getFreshTimestamp())
                .isEqualTo(expected + 1);
    }

    private static InMemoryTestRunner makeRunnerWithTargetTimestamp(long targetTimestamp) {
        return makeRunner(TIMESTAMP_GROUP, "-t", String.valueOf(targetTimestamp), FAST_FORWARD_COMMAND);
    }

    private static InMemoryTestRunner makeRunner(String... args) {
        return new InMemoryTestRunner(FastForwardTimestamp.class, args);
    }

    private static AtlasDbServicesFactory createModuleFactory(InMemoryTimelockServices services) {
        return new AtlasDbServicesFactory() {
            @Override
            public TestAtlasDbServices connect(ServicesConfigModule servicesConfigModule) {
                return DaggerTestAtlasDbServices.builder()
                        .servicesConfigModule(servicesConfigModule)
                        .lockAndTimestampModule(new FakeLockAndTimestampModule(services))
                        .build();
            }
        };
    }

    private static final class FakeLockAndTimestampModule extends LockAndTimestampModule {
        private final InMemoryTimelockServices services;

        public FakeLockAndTimestampModule(InMemoryTimelockServices services) {
            this.services = services;
        }

        @Override
        public LockAndTimestampServices provideLockAndTimestampServices(
                MetricsManager _metricsManager, ServicesConfig _config) {
            return new InMemoryLockAndTimestampServiceFactory(services).createLockAndTimestampServices();
        }
    }
}
