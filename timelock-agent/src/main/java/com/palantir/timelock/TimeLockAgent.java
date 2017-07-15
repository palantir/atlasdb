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
package com.palantir.timelock;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.palantir.atlasdb.timelock.TimeLockResource;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.timelock.config.ImmutableTimeLockDeprecatedConfiguration;
import com.palantir.timelock.config.TimeLockDeprecatedConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;

import io.reactivex.Observable;

public abstract class TimeLockAgent {
    protected final TimeLockInstallConfiguration install;
    protected final Observable<TimeLockRuntimeConfiguration> runtime;

    protected final TimeLockDeprecatedConfiguration deprecated;

    protected final Consumer<Object> registrar;

    public TimeLockAgent(
            TimeLockInstallConfiguration install,
            Observable<TimeLockRuntimeConfiguration> runtime,
            Consumer<Object> registrar) {
        this(install, runtime, ImmutableTimeLockDeprecatedConfiguration.builder().build(), registrar);
    }

    public TimeLockAgent(
            TimeLockInstallConfiguration install,
            Observable<TimeLockRuntimeConfiguration> runtime,
            TimeLockDeprecatedConfiguration deprecated,
            Consumer<Object> registrar) {
        this.install = install;
        this.runtime = runtime.filter(this::configurationFilter);
        this.deprecated = deprecated;
        this.registrar = registrar;
    }

    /**
     * Creates timestamp and lock services for the given client. It is expected that for each client there should
     * only be (up to) one active timestamp service, and one active lock service at any time.
     * @param client Client namespace to create the services for
     * @return Invalidating timestamp and lock services
     */
    public abstract TimeLockServices createInvalidatingTimeLockServices(String client);

    /**
     * Returns whether the given runtimeConfiguration should be permitted to be live reloaded.
     * @param runtimeConfiguration Configuration to check the validity of
     * @return true if and only if the configuration should be allowed
     */
    protected abstract boolean configurationFilter(TimeLockRuntimeConfiguration runtimeConfiguration);

    public void createAndRegisterResources() {
        Set<String> clients =
                Observables.blockingMostRecent(runtime.map(TimeLockRuntimeConfiguration::clients)).get();
        Map<String, TimeLockServices> clientToServices =
                clients.stream().collect(Collectors.toMap(
                        Function.identity(),
                        this::createInvalidatingTimeLockServices));
        registrar.accept(new TimeLockResource(clientToServices));
    }

    // copied the below from WC internally
    // will need this functionality in a utility class somewhere
}
