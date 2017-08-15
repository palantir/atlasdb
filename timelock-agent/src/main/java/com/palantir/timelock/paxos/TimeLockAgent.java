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

package com.palantir.timelock.paxos;

import java.util.function.Consumer;
import java.util.function.Supplier;

import com.palantir.atlasdb.http.BlockingTimeoutExceptionMapper;
import com.palantir.atlasdb.http.NotCurrentLeaderExceptionMapper;
import com.palantir.atlasdb.timelock.TimeLockResource;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.TooManyRequestsExceptionMapper;
import com.palantir.atlasdb.timelock.paxos.ManagedTimestampService;
import com.palantir.atlasdb.timelock.paxos.PaxosResource;
import com.palantir.lock.RemoteLockService;
import com.palantir.remoting2.config.ssl.SslSocketFactories;
import com.palantir.timelock.Observables;
import com.palantir.timelock.clock.ClockSkewMonitorCreator;
import com.palantir.timelock.config.ImmutableTimeLockDeprecatedConfiguration;
import com.palantir.timelock.config.TimeLockDeprecatedConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;

import io.reactivex.Observable;

public class TimeLockAgent {
    private final TimeLockInstallConfiguration install;
    private final Observable<TimeLockRuntimeConfiguration> runtime;
    private final Consumer<Object> registrar;

    private final PaxosResource paxosResource;
    private final PaxosLeadershipCreator leadershipCreator;
    private final LockCreator lockCreator;
    private final PaxosTimestampCreator timestampCreator;
    private final TimeLockServicesCreator timelockCreator;

    public TimeLockAgent(TimeLockInstallConfiguration install,
            Observable<TimeLockRuntimeConfiguration> runtime,
            Consumer<Object> registrar) {
        this(install, runtime, ImmutableTimeLockDeprecatedConfiguration.builder().build(), registrar);
    }

    public TimeLockAgent(TimeLockInstallConfiguration install,
            Observable<TimeLockRuntimeConfiguration> runtime,
            TimeLockDeprecatedConfiguration deprecated,
            Consumer<Object> registrar) {
        this.install = install;
        this.runtime = runtime;
        this.registrar = registrar;

        this.paxosResource = PaxosResource.create(install.paxos().dataDirectory().toString());
        this.leadershipCreator = new PaxosLeadershipCreator(install, runtime, registrar);
        this.lockCreator = new LockCreator(runtime, deprecated);
        this.timestampCreator = new PaxosTimestampCreator(paxosResource,
                PaxosRemotingUtils.getRemoteServerPaths(install),
                PaxosRemotingUtils.getSslConfigurationOptional(install).map(SslSocketFactories::createSslSocketFactory),
                runtime.map(TimeLockRuntimeConfiguration::paxos));
        this.timelockCreator = install.asyncLock().useAsyncLockService()
                ? new AsyncTimeLockServicesCreator(leadershipCreator, install.asyncLock())
                : new LegacyTimeLockServicesCreator(leadershipCreator);
    }

    public void createAndRegisterResources() {
        registerPaxosResource();
        registerExceptionMappers();
        leadershipCreator.registerLeaderElectionService();

        // Finally, register the endpoints associated with the clients.
        registrar.accept(new TimeLockResource(this::createInvalidatingTimeLockServices, Observables.blockingMostRecent(
                runtime.map(conf -> conf.maxNumberOfClients()))));

        ClockSkewMonitorCreator.create(install, registrar).registerClockServices();
    }

    // No runtime configuration at the moment.
    private void registerPaxosResource() {
        registrar.accept(paxosResource);
    }

    private void registerExceptionMappers() {
        registrar.accept(new BlockingTimeoutExceptionMapper());
        registrar.accept(new NotCurrentLeaderExceptionMapper());
        registrar.accept(new TooManyRequestsExceptionMapper());
    }

    /**
     * Creates timestamp and lock services for the given client. It is expected that for each client there should
     * only be (up to) one active timestamp service, and one active lock service at any time.
     * @param client Client namespace to create the services for
     * @return Invalidating timestamp and lock services
     */
    private TimeLockServices createInvalidatingTimeLockServices(String client) {
        Supplier<ManagedTimestampService> rawTimestampServiceSupplier =
                timestampCreator.createPaxosBackedTimestampService(client);
        Supplier<RemoteLockService> rawLockServiceSupplier = lockCreator::createThreadPoolingLockService;

        return timelockCreator.createTimeLockServices(client, rawTimestampServiceSupplier, rawLockServiceSupplier);
    }
}
