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
package com.palantir.atlasdb.timelock.atomix;

import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.timelock.TimeLockServer;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.config.AtomixConfiguration;
import com.palantir.atlasdb.timelock.config.AtomixSslConfiguration;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.remoting2.config.ssl.SslConfiguration;

import io.atomix.AtomixReplica;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.group.DistributedGroup;
import io.atomix.group.LocalMember;
import io.atomix.variables.DistributedLong;
import io.atomix.variables.DistributedValue;

public class AtomixTimeLockServer implements TimeLockServer {
    private static final Logger log = LoggerFactory.getLogger(AtomixTimeLockServer.class);

    private AtomixReplica replica;
    private LocalMember localMember;

    @Override
    public void onStartup(TimeLockServerConfiguration configuration) {
        AtomixConfiguration atomix = (AtomixConfiguration) configuration.algorithm();
        replica = AtomixReplica.builder(new Address(configuration.cluster().localServer()))
                .withStorage(Storage.builder()
                        .withDirectory(atomix.storageDirectory())
                        .withStorageLevel(atomix.storageLevel())
                        .build())
                .withTransport(createTransport(atomix.security()))
                .build();

        startAtomix(configuration);
    }

    private void startAtomix(TimeLockServerConfiguration configuration) {
        AtomixRetryer.getWithRetry(() -> replica.bootstrap(
                configuration.cluster()
                        .servers()
                        .stream()
                        .map(Address::new)
                        .collect(Collectors.toSet())));

        DistributedGroup timeLockGroup = DistributedValues.getTimeLockGroup(replica);
        localMember = AtomixRetryer.getWithRetry(timeLockGroup::join);

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
    }

    @Override
    public void onStop() {
        if (replica != null) {
            AtomixRetryer.getWithRetry(replica::shutdown);
            replica = null;
        }
    }

    @Override
    public void onStartupFailure() {
        onStop();
    }

    @Override
    public TimeLockServices createInvalidatingTimeLockServices(String client, long slowLogTriggerMillis) {
        DistributedValue<LeaderAndTerm> leaderInfo = DistributedValues.getLeaderInfo(replica);
        DistributedLong timestamp = DistributedValues.getTimestampForClient(replica, client);
        Supplier<TimeLockServices> timeLockSupplier = () -> {
            AtomixTimestampService atomixTimestampService = new AtomixTimestampService(timestamp);
            return TimeLockServices.create(atomixTimestampService, LockServiceImpl.create(), atomixTimestampService);
        };
        return InvalidatingLeaderProxy.create(
                localMember,
                leaderInfo,
                timeLockSupplier,
                TimeLockServices.class);
    }

    private static Transport createTransport(Optional<AtomixSslConfiguration> optionalSecurity) {
        NettyTransport.Builder transport = NettyTransport.builder();

        if (!optionalSecurity.isPresent()) {
            return transport.build();
        }

        AtomixSslConfiguration security = optionalSecurity.get();
        SslConfiguration baseSslConfiguration = security.sslConfiguration();
        transport.withSsl()
                .withTrustStorePath(baseSslConfiguration.trustStorePath().toString())
                .withTrustStorePassword(security.trustStorePassword());

        if (isKeystoreSpecified(baseSslConfiguration)) {
            transport.withKeyStorePath(baseSslConfiguration.keyStorePath().get().toString());
            transport.withKeyStorePassword(baseSslConfiguration.keyStorePassword().get());
        }

        return transport.build();
    }

    private static boolean isKeystoreSpecified(SslConfiguration baseSslConfiguration) {
        return baseSslConfiguration.keyStorePath().isPresent() && baseSslConfiguration.keyStorePassword().isPresent();
    }
}
