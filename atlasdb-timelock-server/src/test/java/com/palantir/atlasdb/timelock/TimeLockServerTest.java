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
package com.palantir.atlasdb.timelock;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.config.ImmutableClusterConfiguration;
import com.palantir.atlasdb.timelock.config.TimeLockAlgorithmConfiguration;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;

public class TimeLockServerTest extends ServerImplementationTest {
    private static final String LOCAL_ADDRESS = "localhost:8080";
    private static final String TEST_CLIENT = "test";

    private TimeLockAlgorithmConfiguration algorithmConfiguration;
    private ServerImplementation serverImplementation;
    private TimeLockServerConfiguration timeLockServerConfiguration;

    @Override
    protected TimeLockServerConfiguration getConfiguration() {
        algorithmConfiguration = mock(TimeLockAlgorithmConfiguration.class);
        serverImplementation = mock(ServerImplementation.class);
        when(algorithmConfiguration.createServerImpl()).thenReturn(serverImplementation);
        when(serverImplementation.createInvalidatingTimeLockServices(any())).thenReturn(mock(TimeLockServices.class));

        return new TimeLockServerConfiguration(
                algorithmConfiguration,
                ImmutableClusterConfiguration.builder()
                        .localServer(LOCAL_ADDRESS)
                        .addServers(LOCAL_ADDRESS)
                        .build(),
                ImmutableSet.of(TEST_CLIENT));
    }

    @Override
    protected void verifyPostStartupSuccess() {
        verify(serverImplementation, times(1)).onStartup(any());
    }

    @Override
    protected void verifyPostStartupFailure() {
        verify(serverImplementation, times(1)).onStartupFailure();
        verify(serverImplementation, times(0)).onStop();
    }

    @Override
    protected void verifyPostStop() {
        verify(serverImplementation, times(1)).onStop();
    }
}
