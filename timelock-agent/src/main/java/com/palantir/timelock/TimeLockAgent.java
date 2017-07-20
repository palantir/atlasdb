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
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
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
        this.runtime = runtime;
        this.deprecated = deprecated;
        this.registrar = registrar;
    }

    /**
     * Creates timestamp and lock services for the given client. It is expected that for each client there should
     * only be (up to) one active timestamp service, and one active lock service at any time.
     * @param client Client namespace to create the services for
     * @return Invalidating timestamp and lock services
     */
    abstract TimeLockServices createInvalidatingTimeLockServices(String client);

    public void createAndRegisterResources() {
        // TODO: actually implement this correctly, and not just to make it compile
        runtime.map(config -> config.clients());
        Map<String, TimeLockServices> clientToServices = ImmutableMap.copyOf(Maps.asMap(
                blockingMostRecent(runtime.map(TimeLockRuntimeConfiguration::clients)).get(),
                client -> createInvalidatingTimeLockServices(client)));

        registrar.accept(new TimeLockResource(clientToServices));
    }

    // copied the below from WC internally
    // will need this functionality in a utility class somewhere

    /**
     * Returns a {@link Supplier} that always returns the most recent value on the given {@link Observable}, blocking if
     * the observable has not emitted a value when {@link Supplier#get} is called.
     *
     * @throws NoSuchElementException if the given observable emits no items
     */
    private static <T> Supplier<T> blockingMostRecent(Observable<T> obs) {
        AtomicReference<T> reference = new AtomicReference<>();
        subscribe(obs, reference::set);
        return () -> {
            if (reference.get() == null) {
                return obs.blockingFirst();
            }
            return reference.get();
        };
    }

    private static <T> void subscribe(Observable<T> observable, io.reactivex.functions.Consumer<? super T> onNext) {
        observable.subscribe(onNext);
    }
}
