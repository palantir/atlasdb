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

import java.util.Optional;

import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.remoting.ssl.SslConfiguration;
import com.palantir.timestamp.TimestampService;

import io.atomix.AtomixReplica;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.group.DistributedGroup;
import io.atomix.group.LocalMember;
import io.atomix.variables.DistributedValue;
import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

public class TimeLockServer extends Application<TimeLockServerConfiguration> {
    public static void main(String[] args) throws Exception {
        new TimeLockServer().run(args);
    }

    @Override
    public void run(TimeLockServerConfiguration configuration, Environment environment) {
        AtomixReplica localNode = AtomixReplica.builder(configuration.cluster().localServer())
                .withStorage(Storage.builder()
                        .withDirectory("var/data/atomix")
                        .withStorageLevel(StorageLevel.DISK)
                        .build())
                .withTransport(createTransport(configuration.cluster().security()))
                .build();

        localNode.bootstrap(configuration.cluster().servers()).join();

        DistributedGroup timeLockGroup = DistributedValues.getTimeLockGroup(localNode);
        LocalMember localMember = Futures.getUnchecked(timeLockGroup.join());

        DistributedValue<String> leaderId = DistributedValues.getLeaderId(localNode);
        timeLockGroup.election().onElection(term -> {
            Futures.getUnchecked(leaderId.set(term.leader().id()));
        });

        LeaderFilter leaderFilter = new LeaderFilter(localMember.id(), leaderId);
        environment.jersey().register(leaderFilter);

        TimestampService timestampService = new TimestampResource(DistributedValues.getTimestamp(localNode));
        environment.jersey().register(timestampService);

    }

    private Transport createTransport(Optional<SslConfiguration> optionalSecurity) {
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
