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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.jetty.util.component.LifeCycle;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;

import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;

public abstract class ServerImplementationTest {
    private final TimeLockServer server = new TimeLockServer();
    private final List<LifeCycle.Listener> listeners = new ArrayList<>();

    protected final Environment environment = mock(Environment.class);

    @Before
    public void setUp() {
        setUpEnvironment();
    }

    private void setUpEnvironment() {
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
    public void checkStateAfterStartup() {
        server.run(getConfiguration(), environment);
        verifyPostStartupSuccess();
    }

    @Test
    public void checkStateAfterStartupFailure() {
        RuntimeException expectedException = new RuntimeException("jersey throw");
        when(environment.jersey()).thenThrow(expectedException);

        assertThatThrownBy(() -> server.run(getConfiguration(), environment))
                .isEqualTo(expectedException);
        verifyPostStartupFailure();
    }

    @Test
    public void checkStateAfterStop() {
        server.run(getConfiguration(), environment);

        sendShutdownToListeners();
        listeners.clear();
        verifyPostStop();
    }

    protected abstract TimeLockServerConfiguration getConfiguration();

    protected abstract void verifyPostStartupSuccess();

    protected abstract void verifyPostStartupFailure();

    protected abstract void verifyPostStop();
}
