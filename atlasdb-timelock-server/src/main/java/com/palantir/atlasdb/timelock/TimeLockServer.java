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
import java.util.Set;

import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.component.LifeCycle;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.remoting1.servers.jersey.HttpRemotingJerseyFeature;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

public class TimeLockServer extends Application<TimeLockServerConfiguration> {
    public static void main(String[] args) throws Exception {
        new TimeLockServer().run(args);
    }

    @Override
    public void run(TimeLockServerConfiguration configuration, Environment environment) {
        ServerImplementation serverImpl = configuration.algorithm().createServerImpl();
        try {
            serverImpl.onStartup(configuration);
            registerResources(configuration, environment, serverImpl);
        } catch (Exception e) {
            serverImpl.onStartupFailure();
            throw e;
        }

        environment.lifecycle().addLifeCycleListener(new AbstractLifeCycle.AbstractLifeCycleListener() {
            @Override
            public void lifeCycleStopped(LifeCycle event) {
                serverImpl.onStop();
            }
        });
    }

    private static void registerResources(
            TimeLockServerConfiguration configuration,
            Environment environment,
            ServerImplementation serverImpl) {
        Map<String, TimeLockServices> clientToServices = createTimeLockServicesForClients(
                serverImpl,
                configuration.clients());

        environment.jersey().register(HttpRemotingJerseyFeature.DEFAULT);
        environment.jersey().register(new TimeLockResource(clientToServices));
    }

    private static Map<String, TimeLockServices> createTimeLockServicesForClients(
            ServerImplementation serverImpl,
            Set<String> clients) {
        ImmutableMap.Builder<String, TimeLockServices> clientToServices = ImmutableMap.builder();
        for (String client : clients) {
            clientToServices.put(client, serverImpl.createInvalidatingTimeLockServices(client));
        }
        return clientToServices.build();
    }
}
