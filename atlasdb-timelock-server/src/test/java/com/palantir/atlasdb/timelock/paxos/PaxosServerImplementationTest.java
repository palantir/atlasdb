/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.timelock.paxos;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.ServerImplementationTest;
import com.palantir.atlasdb.timelock.TimeLockResource;
import com.palantir.atlasdb.timelock.config.ImmutableClusterConfiguration;
import com.palantir.atlasdb.timelock.config.ImmutablePaxosConfiguration;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.leader.LeaderElectionService;

public class PaxosServerImplementationTest extends ServerImplementationTest {
    private static final File TEST_DATA_DIRECTORY = new File("testLogs/");
    private static final String LOCAL_ADDRESS_STRING = "localhost:8080";
    private static final Set<String> TEST_CLIENTS = ImmutableSet.of("test");

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(TEST_DATA_DIRECTORY);
    }

    @Override
    protected TimeLockServerConfiguration getConfiguration() {
        return new TimeLockServerConfiguration(
                ImmutablePaxosConfiguration.builder()
                        .paxosDataDir(TEST_DATA_DIRECTORY)
                        .build(),
                ImmutableClusterConfiguration.builder()
                        .addServers(LOCAL_ADDRESS_STRING)
                        .localServer(LOCAL_ADDRESS_STRING)
                        .build(),
                TEST_CLIENTS);
    }

    @Override
    protected void verifyPostStartupSuccess() {
        verify(environment.jersey(), times(1)).register(Mockito.isA(LeaderElectionService.class));
        verify(environment.jersey(), times(1)).register(Mockito.isA(PaxosResource.class));
        verify(environment.jersey(), times(1)).register(Mockito.isA(TimeLockResource.class));
    }

    @Override
    protected void verifyPostStartupFailure() {
        // nothing
    }

    @Override
    protected void verifyPostStop() {
        // nothing
    }
}
