/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.jetty.util.component.LifeCycle;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.config.ImmutableClusterConfiguration;
import com.palantir.atlasdb.timelock.config.TimeLockAlgorithmConfiguration;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.tritium.metrics.MetricRegistries;

import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;

public class TimeLockServerLauncherTest {
    private static final String LOCAL_ADDRESS = "localhost:8080";
    private static final String TEST_CLIENT = "test";

    private final Environment environment = mock(Environment.class);
    private final List<LifeCycle.Listener> listeners = new ArrayList<>();
    private final TimeLockServerLauncher server = new TimeLockServerLauncher();

    private TimeLockServer timeLockServer;
    private TimeLockAlgorithmConfiguration algorithmConfiguration;

    @Before
    public void setUp() {
        setUpMockServerImplementation();
        setUpEnvironment();
    }

    private void setUpMockServerImplementation() {
        algorithmConfiguration = mock(TimeLockAlgorithmConfiguration.class);
        timeLockServer = mock(TimeLockServer.class);
        when(algorithmConfiguration.createServerImpl(environment)).thenReturn(timeLockServer);
        when(timeLockServer.createInvalidatingTimeLockServices(any(), anyLong()))
                .thenReturn(mock(TimeLockServices.class));
    }

    private void setUpEnvironment() {
        when(environment.jersey()).thenReturn(mock(JerseyEnvironment.class));
        when(environment.metrics()).thenReturn(MetricRegistries.createWithHdrHistogramReservoirs());

        LifecycleEnvironment lifecycle = mock(LifecycleEnvironment.class);
        when(environment.lifecycle()).thenReturn(lifecycle);
        doAnswer(invocation -> listeners.add((LifeCycle.Listener) invocation.getArguments()[0]))
                .when(lifecycle).addLifeCycleListener(any());
    }

    @After
    public void sendShutdownToListeners() {
        LifeCycle event = mock(LifeCycle.class);
        listeners.forEach(listener -> listener.lifeCycleStopping(event));
        listeners.forEach(listener -> listener.lifeCycleStopped(event));
    }

    @Test
    public void verifyOnStartupIsCalledExactlyOnceOnSuccessfulStartup() {
        server.run(getConfiguration(), environment);
        verify(timeLockServer, times(1)).onStartup(any());
    }

    @Test
    public void verifyOnStartupFailureIsCalledExactlyOnceIfStartupFails() {
        causeFailedStartup();
        verify(timeLockServer, times(1)).onStartupFailure();
    }

    @Test
    public void verifyOnStopIsNeverCalledIfStartupFails() {
        causeFailedStartup();
        verify(timeLockServer, never()).onStop();
    }

    private void causeFailedStartup() {
        RuntimeException expectedException = new RuntimeException("jersey throw");
        when(environment.jersey()).thenThrow(expectedException);

        assertThatThrownBy(() -> server.run(getConfiguration(), environment))
                .isEqualTo(expectedException);
    }

    @Test
    public void verifyOnStopIsCalledExactlyOnceIfServerShutsDown() {
        server.run(getConfiguration(), environment);

        sendShutdownToListeners();
        listeners.clear();
        verify(timeLockServer, times(1)).onStop();
    }

    private TimeLockServerConfiguration getConfiguration() {
        return new TimeLockServerConfiguration(
                algorithmConfiguration,
                ImmutableClusterConfiguration.builder()
                        .localServer(LOCAL_ADDRESS)
                        .addServers(LOCAL_ADDRESS)
                        .build(),
                ImmutableSet.of(TEST_CLIENT),
                false);
    }
}
