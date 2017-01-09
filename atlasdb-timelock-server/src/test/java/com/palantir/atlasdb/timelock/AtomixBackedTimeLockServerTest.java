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
package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.jetty.util.component.LifeCycle;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.config.ImmutableAtomixConfiguration;
import com.palantir.atlasdb.timelock.config.ImmutableClusterConfiguration;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;

import io.atomix.catalyst.transport.Address;
import io.atomix.copycat.server.storage.StorageLevel;
import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;

@Ignore("Observed ConcurrentModificationException-related flakes (e.g. build #5407 on CircleCI)."
        + "Fixed in atomix/copycat#231, but not part of Copycat 1.1.4 which we use.")
public class AtomixBackedTimeLockServerTest {
    private static final String LOCAL_ADDRESS_STRING = "localhost:12345";
    private static final Address LOCAL_ADDRESS = new Address(LOCAL_ADDRESS_STRING);
    private static final TimeLockServerConfiguration TIMELOCK_CONFIG = new TimeLockServerConfiguration(
            ImmutableAtomixConfiguration.builder()
                    .storageLevel(StorageLevel.MEMORY)
                    .build(),
            ImmutableClusterConfiguration.builder()
                    .localServer(LOCAL_ADDRESS_STRING)
                    .addServers(LOCAL_ADDRESS_STRING)
                    .build(),
            ImmutableSet.of(String.format("%s:%s", LOCAL_ADDRESS.host(), LOCAL_ADDRESS.port())));

    private final TimeLockServer server = new TimeLockServer();
    private final Environment environment = mock(Environment.class);
    private final List<LifeCycle.Listener> listeners = new ArrayList<>();

    @Before
    public void setupEnvironment() {
        when(environment.jersey()).thenReturn(mock(JerseyEnvironment.class));

        LifecycleEnvironment lifecycle = mock(LifecycleEnvironment.class);
        when(environment.lifecycle()).thenReturn(lifecycle);
        doAnswer(inv -> listeners.add((LifeCycle.Listener) inv.getArguments()[0]))
                .when(lifecycle).addLifeCycleListener(any());
    }

    @After
    public void sendShutdownToListeners() {
        LifeCycle event = mock(LifeCycle.class);
        listeners.forEach(listener -> listener.lifeCycleStopping(event));
        listeners.forEach(listener -> listener.lifeCycleStopped(event));
    }

    @Test
    public void atomixIsRunningAfterSuccessfulStartup() throws IOException {
        server.run(TIMELOCK_CONFIG, environment);
        tryToConnectToAtomixPort();
    }

    @Test
    public void atomixIsShutdownWhenAnErrorOccurredDuringRun() {
        RuntimeException expectedException = new RuntimeException("jersey throw");
        when(environment.jersey()).thenThrow(expectedException);

        assertThatThrownBy(() -> server.run(TIMELOCK_CONFIG, environment))
                .isEqualTo(expectedException);

        assertThatThrownBy(AtomixBackedTimeLockServerTest::tryToConnectToAtomixPort)
                .isInstanceOf(ConnectException.class);
    }

    @Test
    public void atomixIsShutdownWhenTheLifecycleEventsAreCalled() {
        server.run(TIMELOCK_CONFIG, environment);

        sendShutdownToListeners();
        listeners.clear();

        assertThatThrownBy(AtomixBackedTimeLockServerTest::tryToConnectToAtomixPort)
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("Connection refused");
    }

    private static void tryToConnectToAtomixPort() throws IOException {
        new Socket(LOCAL_ADDRESS.host(), LOCAL_ADDRESS.port());
    }
}
