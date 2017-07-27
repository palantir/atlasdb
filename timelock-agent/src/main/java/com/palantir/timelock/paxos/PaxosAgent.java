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
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.TooManyRequestsExceptionMapper;
import com.palantir.atlasdb.timelock.paxos.ManagedTimestampService;
import com.palantir.atlasdb.timelock.paxos.PaxosResource;
import com.palantir.lock.RemoteLockService;
import com.palantir.remoting2.config.ssl.SslSocketFactories;
import com.palantir.timelock.TimeLockAgent;
import com.palantir.timelock.config.ImmutablePaxosRuntimeConfiguration;
import com.palantir.timelock.config.ImmutableTimeLockDeprecatedConfiguration;
import com.palantir.timelock.config.PaxosInstallConfiguration;
import com.palantir.timelock.config.TimeLockDeprecatedConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;

import io.reactivex.Observable;

public class PaxosAgent extends TimeLockAgent {
    private final PaxosInstallConfiguration paxosInstall;

    private final PaxosResource paxosResource;
    private final PaxosLeadershipCreator leadershipCreator;
    private final LockCreator lockCreator;
    private final PaxosTimestampCreator timestampCreator;
    private final TimeLockServicesCreator timelockCreator;

    public PaxosAgent(TimeLockInstallConfiguration install,
            Observable<TimeLockRuntimeConfiguration> runtime,
            Consumer<Object> registrar) {
        this(install, runtime, ImmutableTimeLockDeprecatedConfiguration.builder().build(), registrar);
    }

    public PaxosAgent(TimeLockInstallConfiguration install,
            Observable<TimeLockRuntimeConfiguration> runtime,
            TimeLockDeprecatedConfiguration deprecated,
            Consumer<Object> registrar) {
        super(install, runtime, deprecated, registrar);
        this.paxosInstall = install.algorithm();

        this.paxosResource = PaxosResource.create(paxosInstall.dataDirectory().toString());
        this.leadershipCreator = new PaxosLeadershipCreator(install, runtime, registrar);
        this.lockCreator = new LockCreator(runtime, deprecated);
        this.timestampCreator = new PaxosTimestampCreator(paxosResource,
                PaxosRemotingUtils.getRemoteServerPaths(install),
                PaxosRemotingUtils.getSslConfigurationOptional(install).map(SslSocketFactories::createSslSocketFactory),
                runtime.map(x -> x.algorithm().orElse(ImmutablePaxosRuntimeConfiguration.builder().build())));
        this.timelockCreator = install.asyncLock().useAsyncLockService()
                ? new AsyncTimeLockServicesCreator(leadershipCreator, install.asyncLock())
                : new LegacyTimeLockServicesCreator(leadershipCreator);
    }

    @Override
    public void createAndRegisterResources() {
        registerPaxosResource();
        registerPaxosExceptionMappers();
        leadershipCreator.registerLeaderElectionService();

        // Finally, register the endpoints associated with the clients.
        super.createAndRegisterResources();
    }

    // No runtime configuration at the moment.
    private void registerPaxosResource() {
        registrar.accept(paxosResource);
    }

    private void registerPaxosExceptionMappers() {
        registrar.accept(new BlockingTimeoutExceptionMapper());
        registrar.accept(new NotCurrentLeaderExceptionMapper());
        registrar.accept(new TooManyRequestsExceptionMapper());
    }

    @Override
    protected TimeLockServices createInvalidatingTimeLockServices(String client) {
        Supplier<ManagedTimestampService> rawTimestampServiceSupplier =
                timestampCreator.createPaxosBackedTimestampService(client);
        Supplier<RemoteLockService> rawLockServiceSupplier = lockCreator::createThreadPoolingLockService;

        return timelockCreator.createTimeLockServices(client, rawTimestampServiceSupplier, rawLockServiceSupplier);
    }
}
