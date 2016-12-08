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
package com.palantir.atlasdb.timelock.copycat;

import java.util.Optional;
import java.util.function.Supplier;

import com.palantir.atlasdb.timelock.ServerImplementation;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.remoting1.config.ssl.SslConfiguration;

import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.client.ConnectionStrategies;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.RecoveryStrategies;
import io.atomix.copycat.client.ServerSelectionStrategies;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.storage.Storage;

public class CopycatServerImplementation implements ServerImplementation {
    private CopycatServer server;
    private CopycatClient client;

    @Override
    public void onStart(TimeLockServerConfiguration configuration) {
        server = CopycatServer.builder(configuration.cluster().localServer())
                .withStorage(Storage.builder()
                        .withDirectory(configuration.atomix().storageDirectory())
                        .withStorageLevel(configuration.atomix().storageLevel())
                        .build())
                .withTransport(createTransport(configuration.atomix().security()))
                .withStateMachine(TimeLockStateMachine::new)
                .build();
        client = CopycatClient.builder()
                .withTransport(createTransport(configuration.atomix().security()))
                .withRecoveryStrategy(RecoveryStrategies.CLOSE)
                .withConnectionStrategy(ConnectionStrategies.FIBONACCI_BACKOFF)
                .withServerSelectionStrategy(ServerSelectionStrategies.LEADER)
                .build();

        server.bootstrap(configuration.cluster().servers()).join();
        client.connect(configuration.cluster().servers()).join();

        client.onStateChange(state -> {
            if (state == CopycatClient.State.CLOSED) {
                client.connect().join();
            }
        });

        // This is to install the first leader correctly.
        // Updating to an old term does nothing.
        client.submit(ImmutableUpdateLeaderCommand.builder()
                .leader(server.cluster().leader().id())
                .term(server.cluster().term())
                .build()).join();

        server.cluster().onLeaderElection(member -> {
            if (member.equals(server.cluster().member())) {
                client.submit(ImmutableUpdateLeaderCommand.builder()
                        .leader(member.id())
                        .term(server.cluster().term())
                        .build());
            }
        });
    }

    @Override
    public void onStop() {
        if (client != null) {
            client.close();
            client = null;
        }
        if (server != null) {
            server.shutdown().join();
        }
    }

    @Override
    public void onFail() {
        onStop();
    }

    @Override
    public TimeLockServices createInvalidatingTimeLockServices(String clientName) {
        Supplier<TimeLockServices> timeLockSupplier = () -> TimeLockServices.create(
                new CopycatTimestampService(client, clientName),
                LockServiceImpl.create());
        return CopycatInvalidatingLeaderProxy.create(server, client, timeLockSupplier, TimeLockServices.class);
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
