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
package com.palantir.atlasdb.timelock.atomix;

import java.util.Optional;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.timelock.ServerImplementation;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.remoting1.config.ssl.SslConfiguration;

import io.atomix.AtomixReplica;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.group.DistributedGroup;
import io.atomix.group.LocalMember;
import io.atomix.variables.DistributedLong;
import io.atomix.variables.DistributedValue;

public class AtomixServerImplementation implements ServerImplementation {
    private static final Logger log = LoggerFactory.getLogger(AtomixServerImplementation.class);

    private AtomixReplica replica;
    private LocalMember localMember;

    @Override
    public void onStart(TimeLockServerConfiguration configuration) {
        replica = AtomixReplica.builder(configuration.cluster().localServer())
                .withStorage(Storage.builder()
                        .withDirectory(configuration.atomix().storageDirectory())
                        .withStorageLevel(configuration.atomix().storageLevel())
                        .build())
                .withTransport(createTransport(configuration.atomix().security()))
                .build();
        AtomixRetryer.getWithRetry(() -> replica.bootstrap(configuration.cluster().servers()));

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
    public void onFail() {
        onStop();
    }

    @Override
    public TimeLockServices createInvalidatingTimeLockServices(String client) {
        DistributedValue<LeaderAndTerm> leaderInfo = DistributedValues.getLeaderInfo(replica);
        DistributedLong timestamp = DistributedValues.getTimestampForClient(replica, client);
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
