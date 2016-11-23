/**
 * Copyright 2016 Palantir Technologies
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.palantir.atlasdb.cli.command.timestamp.FastForwardTimestamp;
import com.palantir.atlasdb.cli.runner.InMemoryTestRunner;
import com.palantir.atlasdb.factory.ImmutableLockAndTimestampServices;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.services.AtlasDbServicesFactory;
import com.palantir.atlasdb.services.LockAndTimestampModule;
import com.palantir.atlasdb.services.ServicesConfig;
import com.palantir.atlasdb.services.ServicesConfigModule;
import com.palantir.atlasdb.services.test.DaggerTestAtlasDbServices;
import com.palantir.atlasdb.services.test.TestAtlasDbServices;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.PersistentTimestampService;

import io.airlift.airline.Command;

public class TestFastForwardTimestampCommand {
    private static final String FAST_FORWARD_COMMAND = FastForwardTimestamp.class.getAnnotation(Command.class).name();
    private static final Long TEST_TIMESTAMP = 400L;

    private static AtlasDbServicesFactory moduleFactory = createModuleFactory();

    @Test
    public void testFastForwardTimestamp() throws Exception {
        InMemoryTestRunner runner = makeRunner("timestamp", "-t", Long.toString(TEST_TIMESTAMP), FAST_FORWARD_COMMAND);
        AtlasDbServices atlasDbServices = runner.connect(moduleFactory);

        long currentTimestamp = atlasDbServices.getTimestampService().getFreshTimestamp();
        assertThat(currentTimestamp).isLessThan(TEST_TIMESTAMP);

        String response = runner.run();
        assertThat(response).contains("Timestamp successfully fast-forwarded to 400");

        assertThat(atlasDbServices.getTimestampService().getFreshTimestamp()).isEqualTo(TEST_TIMESTAMP + 1);
    }

    private static InMemoryTestRunner makeRunner(String... args) {
        return new InMemoryTestRunner(FastForwardTimestamp.class, args);
    }

    private static AtlasDbServicesFactory createModuleFactory() {
        return new AtlasDbServicesFactory() {
            @Override
            public TestAtlasDbServices connect(ServicesConfigModule servicesConfigModule) {
                return DaggerTestAtlasDbServices.builder()
                        .servicesConfigModule(servicesConfigModule)
                        .lockAndTimestampModule(new FakeLockAndTimestampModule())
                        .build();
            }
        };
    }

    private static class FakeLockAndTimestampModule extends LockAndTimestampModule {
        private PersistentTimestampService timestampService = mock(PersistentTimestampService.class);

        FakeLockAndTimestampModule() {
            InMemoryTimestampService inMemoryTimestampService = new InMemoryTimestampService();
            when(timestampService.getFreshTimestamp()).then(inv -> inMemoryTimestampService.getFreshTimestamp());
            doAnswer(inv -> {
                long fastForwardArg = (Long) inv.getArguments()[0];
                long currentTimestamp = inMemoryTimestampService.getFreshTimestamp();
                long amountToIncreaseBy = fastForwardArg - currentTimestamp;
                inMemoryTimestampService.getFreshTimestamps((int) amountToIncreaseBy);
                return null;
            }).when(timestampService).fastForwardTimestamp(anyLong());
        }

        @Override
        public TransactionManagers.LockAndTimestampServices provideLockAndTimestampServices(ServicesConfig config) {
            return ImmutableLockAndTimestampServices.builder()
                    .lock(LockServiceImpl.create())
                    .time(timestampService)
                    .build();
        }
    }
}
