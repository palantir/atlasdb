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

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.http.BlockingTimeoutExceptionMapper;
import com.palantir.atlasdb.http.NotCurrentLeaderExceptionMapper;
import com.palantir.atlasdb.timelock.TimeLockResource;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.TooManyRequestsExceptionMapper;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.atlasdb.timelock.paxos.ManagedTimestampService;
import com.palantir.atlasdb.timelock.paxos.PaxosResource;
import com.palantir.atlasdb.util.JavaSuppliers;
import com.palantir.lock.LockService;
import com.palantir.remoting3.config.ssl.SslSocketFactories;
import com.palantir.timelock.TimeLockStatus;
import com.palantir.timelock.clock.ClockSkewMonitorCreator;
import com.palantir.timelock.config.DatabaseTsBoundPersisterConfiguration;
import com.palantir.timelock.config.PaxosTsBoundPersisterConfiguration;
import com.palantir.timelock.config.TimeLockDeprecatedConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;
import com.palantir.timelock.config.TsBoundPersisterConfiguration;

public final class TimeLockAgent {
    private static final Long SCHEMA_VERSION = 1L;

    private final TimeLockInstallConfiguration install;
    private final Supplier<TimeLockRuntimeConfiguration> runtime;
    private final Consumer<Object> registrar;

    private final PaxosResource paxosResource;
    private final PaxosLeadershipCreator leadershipCreator;
    private final LockCreator lockCreator;
    private final TimestampCreator timestampCreator;
    private final TimeLockServicesCreator timelockCreator;

    private Supplier<LeaderPingHealthCheck> healthCheckSupplier;

    public static TimeLockAgent create(TimeLockInstallConfiguration install,
            Supplier<TimeLockRuntimeConfiguration> runtime,
            TimeLockDeprecatedConfiguration deprecated,
            Consumer<Object> registrar) {
        TimeLockAgent agent = new TimeLockAgent(install, runtime, deprecated, registrar);
        agent.createAndRegisterResources();
        return agent;
    }

    private TimeLockAgent(TimeLockInstallConfiguration install,
            Supplier<TimeLockRuntimeConfiguration> runtime,
            TimeLockDeprecatedConfiguration deprecated,
            Consumer<Object> registrar) {
        this.install = install;
        this.runtime = runtime;
        this.registrar = registrar;

        this.paxosResource = PaxosResource.create(install.paxos().dataDirectory().toString());
        this.leadershipCreator = new PaxosLeadershipCreator(install, runtime, registrar);
        this.lockCreator = new LockCreator(runtime, deprecated);
        this.timestampCreator = getTimestampCreator();
        this.timelockCreator = install.asyncLock().useAsyncLockService()
                ? new AsyncTimeLockServicesCreator(leadershipCreator, install.asyncLock())
                : new LegacyTimeLockServicesCreator(leadershipCreator);
    }

    private TimestampCreator getTimestampCreator() {
        TsBoundPersisterConfiguration timestampBoundPersistence = install.timestampBoundPersistence();
        if (PaxosTsBoundPersisterConfiguration.class.isInstance(timestampBoundPersistence)) {
            return getPaxosTimestampCreator();
        } else if (DatabaseTsBoundPersisterConfiguration.class.isInstance(timestampBoundPersistence)) {
            return new DbBoundTimestampCreator(
                    ((DatabaseTsBoundPersisterConfiguration) timestampBoundPersistence)
                            .keyValueServiceConfig());
        }
        throw new RuntimeException(String.format("Unknown TsBoundPersisterConfiguration found %s",
                timestampBoundPersistence.getClass()));
    }

    private PaxosTimestampCreator getPaxosTimestampCreator() {
        return new PaxosTimestampCreator(paxosResource,
                PaxosRemotingUtils.getRemoteServerPaths(install),
                PaxosRemotingUtils.getSslConfigurationOptional(install).map(SslSocketFactories::createSslSocketFactory),
                JavaSuppliers.compose(TimeLockRuntimeConfiguration::paxos, runtime));
    }

    private void createAndRegisterResources() {
        registerPaxosResource();
        registerExceptionMappers();
        leadershipCreator.registerLeaderElectionService();

        // Finally, register the health check, and endpoints associated with the clients.
        healthCheckSupplier = leadershipCreator.getHealthCheck();
        registrar.accept(
                new TimeLockResource(this::createInvalidatingTimeLockServices,
                        JavaSuppliers.compose(TimeLockRuntimeConfiguration::maxNumberOfClients, runtime)));

        ClockSkewMonitorCreator.create(install, registrar).registerClockServices();
    }

    @SuppressWarnings("unused") // used by external health checks
    public TimeLockStatus getStatus() {
        return healthCheckSupplier.get().getStatus();
    }

    @SuppressWarnings("unused")
    public long getSchemaVersion() {
        // So far there's only been one schema version. For future schema versions, we will have to persist the version
        // to disk somehow, so that we can check if var/data/paxos will have data in the expected format.
        return SCHEMA_VERSION;
    }

    @SuppressWarnings("unused")
    public long getLatestSchemaVersion() {
        return SCHEMA_VERSION;
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
        List<String> uris = install.cluster().cluster().uris();
        ImmutableLeaderConfig leaderConfig = ImmutableLeaderConfig.builder()
                .addLeaders(uris.toArray(new String[uris.size()]))
                .localServer(install.cluster().localServer())
                .sslConfiguration(PaxosRemotingUtils.getSslConfigurationOptional(install))
                .quorumSize(PaxosRemotingUtils.getQuorumSize(uris))
                .build();

        Supplier<ManagedTimestampService> rawTimestampServiceSupplier = timestampCreator
                .createTimestampService(client, leaderConfig);
        Supplier<LockService> rawLockServiceSupplier = lockCreator::createThreadPoolingLockService;

        LockLog.setSlowLockThresholdMillis(
                JavaSuppliers.compose(TimeLockRuntimeConfiguration::slowLockLogTriggerMillis, runtime));
        return timelockCreator.createTimeLockServices(client, rawTimestampServiceSupplier, rawLockServiceSupplier);
    }
}
