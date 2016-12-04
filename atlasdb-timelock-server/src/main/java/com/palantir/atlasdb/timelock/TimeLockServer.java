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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.atlasdb.timelock.copycat.CopycatInvalidatingLeaderProxy;
import com.palantir.atlasdb.timelock.copycat.CopycatTimestampService;
import com.palantir.atlasdb.timelock.copycat.TimeLockStateMachine;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.remoting1.config.ssl.SslConfiguration;
import com.palantir.remoting1.servers.jersey.HttpRemotingJerseyFeature;

import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.storage.Storage;
import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

public class TimeLockServer extends Application<TimeLockServerConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(TimeLockServer.class);

    public static void main(String[] args) throws Exception {
        new TimeLockServer().run(args);
    }

    @Override
    public void run(TimeLockServerConfiguration configuration, Environment environment) {
        CopycatServer localNode = CopycatServer.builder(configuration.cluster().localServer())
                .withStorage(Storage.builder()
                        .withDirectory(configuration.atomix().storageDirectory())
                        .withStorageLevel(configuration.atomix().storageLevel())
                        .build())
                .withTransport(createTransport(configuration.atomix().security()))
                .withStateMachine(() -> new TimeLockStateMachine())
                .build();
        CopycatClient localClient = CopycatClient.builder(configuration.cluster().localServer())
                .withTransport(createTransport(configuration.atomix().security()))
                .build();

        localNode.bootstrap(configuration.cluster().servers()).join();
        localClient.connect();

        Map<String, TimeLockServices> clientToServices = new HashMap<>();
        for (String client : configuration.clients()) {
            Supplier<TimeLockServices> timeLockSupplier = () -> TimeLockServices.create(
                    new CopycatTimestampService(localClient, client),
                    LockServiceImpl.create());
            TimeLockServices timeLockServices = createInvalidatingTimeLockServices(
                    localNode,
                    timeLockSupplier);
            clientToServices.put(client, timeLockServices);
        }

        environment.jersey().register(HttpRemotingJerseyFeature.DEFAULT);
        environment.jersey().register(new TimeLockResource(clientToServices));
    }

    private static TimeLockServices createInvalidatingTimeLockServices(CopycatServer server,
            Supplier<TimeLockServices> timeLockSupplier) {
        return CopycatInvalidatingLeaderProxy.create(server, timeLockSupplier, TimeLockServices.class);
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
