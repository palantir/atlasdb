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

import org.junit.BeforeClass;
import org.junit.Test;

import com.palantir.atlasdb.cli.command.timestamp.FastForwardTimestamp;
import com.palantir.atlasdb.cli.runner.InMemoryTestRunner;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.services.AtlasDbServicesFactory;
import com.palantir.atlasdb.services.ServicesConfigModule;
import com.palantir.atlasdb.services.test.DaggerTestAtlasDbServices;
import com.palantir.atlasdb.services.test.TestAtlasDbServices;

import io.airlift.airline.Command;

public class TestFastForwardTimestampCommand {
    private static final String FAST_FORWARD_COMMAND = FastForwardTimestamp.class.getAnnotation(Command.class).name();
    private static final Long testTimestamp = 400L;
    private static AtlasDbServicesFactory moduleFactory;

    @BeforeClass
    public static void oneTimeSetup() throws Exception {
        moduleFactory = new AtlasDbServicesFactory() {
            @Override
            public TestAtlasDbServices connect(ServicesConfigModule servicesConfigModule) {
                return DaggerTestAtlasDbServices.builder()
                        .servicesConfigModule(servicesConfigModule)
                        .build();
            }
        };
    }

    private InMemoryTestRunner makeRunner(String... args) {
        return new InMemoryTestRunner(FastForwardTimestamp.class, args);
    }

    @Test
    public void testFastForwardTimestamp() throws Exception {
        InMemoryTestRunner runner = makeRunner("timestamp", "-t", Long.toString(testTimestamp), FAST_FORWARD_COMMAND);
        AtlasDbServices atlasDbServices = runner.connect(moduleFactory);

        long currentTimestamp = atlasDbServices.getTimestampService().getFreshTimestamp();
        assertThat(currentTimestamp).isLessThan(testTimestamp);

        String response = runner.run();
        assertThat(response).contains("Timestamp succesfully fast-forwarded to 400");

        assertThat(atlasDbServices.getTimestampService().getFreshTimestamp()).isEqualTo(testTimestamp + 1);
    }
}
