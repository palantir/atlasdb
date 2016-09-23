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
package com.palantir.atlasdb.server;

import java.util.Map;

import org.glassfish.jersey.server.model.Resource;

import com.google.common.base.Optional;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.factory.ImmutableLockAndTimestampServices;
import com.palantir.atlasdb.factory.Leaders;
import com.palantir.atlasdb.factory.LockAndTimestampServices;
import com.palantir.atlasdb.factory.ServiceDiscoveringAtlasSupplier;
import com.palantir.atlasdb.server.config.AtlasDbServerConfiguration;
import com.palantir.atlasdb.server.config.ClientConfig;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.proxy.AwaitingLeadershipProxy;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.TimestampService;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;
import one.util.streamex.StreamEx;

public class AtlasDbServer extends Application<AtlasDbServerConfiguration> {
    public static void main(String[] args) throws Exception {
        new AtlasDbServer().run(args);
    }

    @Override
    public void run(AtlasDbServerConfiguration config, Environment environment) {
        Map<String, ServiceDiscoveringAtlasSupplier> clientToAtlasInstance = getClientToAtlasInstanceMapping(config);
        clientToAtlasInstance.forEach((client, atlasFactory) -> {
            LockAndTimestampServices services = constructServicesFromAtlasInstance(atlasFactory, config, environment);

            Resource builtResource = Resources.getInstancedResourceAtPath(client, new ClientResource(services));
            environment.jersey().getResourceConfig().registerResources(builtResource);

            environment.healthChecks().register(client, new KeyValueServiceHealthCheck(atlasFactory));
        });
    }

    private static Map<String, ServiceDiscoveringAtlasSupplier> getClientToAtlasInstanceMapping(
            AtlasDbServerConfiguration config) {
        Optional<LeaderConfig> leaderConfig = Optional.of(config.cluster().toLeaderConfig());
        return StreamEx.of(config.clients())
                .mapToEntry(ClientConfig::client, ClientConfig::keyValueService)
                .mapValues(kvsConfig -> new ServiceDiscoveringAtlasSupplier(kvsConfig, leaderConfig))
                .toMap();
    }

    private static LockAndTimestampServices constructServicesFromAtlasInstance(
            ServiceDiscoveringAtlasSupplier atlasFactory,
            AtlasDbServerConfiguration config,
            Environment environment) {
        LeaderElectionService leader = Leaders.create(
                environment.jersey()::register,
                config.cluster().toLeaderConfig());

        return ImmutableLockAndTimestampServices.builder()
                .lock(AwaitingLeadershipProxy.newProxyInstance(
                        RemoteLockService.class,
                        LockServiceImpl::create,
                        leader))
                .time(AwaitingLeadershipProxy.newProxyInstance(
                        TimestampService.class,
                        atlasFactory::getTimestampService,
                        leader))
                .build();
    }
}
