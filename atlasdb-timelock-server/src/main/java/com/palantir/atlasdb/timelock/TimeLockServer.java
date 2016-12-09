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

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.component.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.timelock.atomix.AtomixRetryer;
import com.palantir.atlasdb.timelock.atomix.AtomixTimestampService;
import com.palantir.atlasdb.timelock.atomix.DistributedValues;
import com.palantir.atlasdb.timelock.atomix.ImmutableLeaderAndTerm;
import com.palantir.atlasdb.timelock.atomix.InvalidatingLeaderProxy;
import com.palantir.atlasdb.timelock.atomix.LeaderAndTerm;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.remoting1.config.ssl.SslConfiguration;
import com.palantir.remoting1.servers.jersey.HttpRemotingJerseyFeature;

import io.atomix.AtomixReplica;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.group.DistributedGroup;
import io.atomix.group.LocalMember;
import io.atomix.variables.DistributedLong;
import io.atomix.variables.DistributedValue;
import io.dropwizard.Application;
import io.dropwizard.java8.Java8Bundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class TimeLockServer extends Application<TimeLockServerConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(TimeLockServer.class);

    public static void main(String[] args) throws Exception {
        new TimeLockServer().run(args);
    }

    @Override
    public void initialize(Bootstrap<TimeLockServerConfiguration> bootstrap) {
        bootstrap.addBundle(new Java8Bundle());
    }

    @Override
    public void run(TimeLockServerConfiguration configuration, Environment environment) {
        AtomixReplica replica = AtomixReplica.builder(configuration.cluster().localServer())
                .withStorage(Storage.builder()
                        .withDirectory(configuration.atomix().storageDirectory())
                        .withStorageLevel(configuration.atomix().storageLevel())
                        .build())
                .withTransport(createTransport(configuration.atomix().security()))
                .build();
        try {
            AtomixRetryer.getWithRetry(() -> replica.bootstrap(configuration.cluster().servers()));
            run(configuration, environment, replica);
        } catch (Exception e) {
            AtomixRetryer.getWithRetry(replica::shutdown);
            throw e;
        }

        environment.lifecycle().addLifeCycleListener(new AbstractLifeCycle.AbstractLifeCycleListener() {
            @Override
            public void lifeCycleStopped(LifeCycle event) {
                AtomixRetryer.getWithRetry(replica::shutdown);
            }
        });
    }

    private static void run(TimeLockServerConfiguration configuration, Environment environment, AtomixReplica replica) {
        DistributedGroup timeLockGroup = DistributedValues.getTimeLockGroup(replica);
        LocalMember localMember = AtomixRetryer.getWithRetry(timeLockGroup::join);

        DistributedValue<LeaderAndTerm> leaderInfo = DistributedValues.getLeaderInfo(replica);
        timeLockGroup.election().onElection(term -> {
            LeaderAndTerm newLeaderInfo = ImmutableLeaderAndTerm.of(term.term(), term.leader().id());
            while (true) {
                LeaderAndTerm currentLeaderInfo = AtomixRetryer.getWithRetry(leaderInfo::get);
                if (currentLeaderInfo != null && newLeaderInfo.term() <= currentLeaderInfo.term()) {
                    log.info("Not setting the leader to {} since it is not newer than the current leader {}",
                            newLeaderInfo, currentLeaderInfo);
                    break;
                }
                log.debug("Updating the leader from {} to {}", currentLeaderInfo, newLeaderInfo);
                if (leaderInfo.compareAndSet(currentLeaderInfo, newLeaderInfo).join()) {
                    log.info("Leader has been set to {}", newLeaderInfo);
                    break;
                }
            }
        });

        Map<String, TimeLockServices> clientToServices = createTimeLockServicesForClients(
                replica,
                configuration.clients(),
                localMember,
                leaderInfo);

        environment.jersey().register(HttpRemotingJerseyFeature.DEFAULT);
        environment.jersey().register(new TimeLockResource(clientToServices));
    }

    private static Map<String, TimeLockServices> createTimeLockServicesForClients(
            AtomixReplica replica,
            Set<String> clients,
            LocalMember localMember,
            DistributedValue<LeaderAndTerm> leaderInfo) {
        ImmutableMap.Builder<String, TimeLockServices> clientToServices = ImmutableMap.builder();
        for (String client : clients) {
            DistributedLong timestamp = DistributedValues.getTimestampForClient(replica, client);
            TimeLockServices timeLockServices = createInvalidatingTimeLockServices(
                    localMember,
                    leaderInfo,
                    timestamp);
            clientToServices.put(client, timeLockServices);
        }
        return clientToServices.build();
    }

    private static TimeLockServices createInvalidatingTimeLockServices(
            LocalMember localMember,
            DistributedValue<LeaderAndTerm> leaderInfo,
            DistributedLong timestamp) {
        Supplier<TimeLockServices> timeLockSupplier = () -> TimeLockServices.create(
                new AtomixTimestampService(timestamp),
                LockServiceImpl.create());
        return InvalidatingLeaderProxy.create(
                localMember,
                leaderInfo,
                timeLockSupplier,
                TimeLockServices.class);
    }

    private static Transport createTransport(Optional<SslConfiguration> optionalSecurity) {
        NettyTransport.Builder transport = NettyTransport.builder();

        if (!optionalSecurity.isPresent()) {
            return transport.build();
        }

        SslConfiguration security = optionalSecurity.get();
        transport.withSsl()
                .withTrustStorePath(security.trustStorePath().toString());

        if (security.keyStorePath().isPresent()) {
            transport.withKeyStorePath(security.keyStorePath().get().toString());
        }

        if (security.keyStorePassword().isPresent()) {
            transport.withKeyStorePassword(security.keyStorePassword().get());
        }

        return transport.build();
    }
}
