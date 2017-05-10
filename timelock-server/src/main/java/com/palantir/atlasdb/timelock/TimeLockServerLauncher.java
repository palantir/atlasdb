/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.remoting1.servers.jersey.HttpRemotingJerseyFeature;
import com.palantir.tritium.metrics.MetricRegistries;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import java.io.IOException;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.component.LifeCycle;

public class TimeLockServerLauncher extends Application<TimeLockServerConfiguration> {
    public static void main(String[] args) throws Exception {
        new TimeLockServerLauncher().run(args);
    }

    @Override
    public void initialize(Bootstrap<TimeLockServerConfiguration> bootstrap) {
        MetricRegistry metricRegistry = MetricRegistries.createWithHdrHistogramReservoirs();
        AtlasDbMetrics.setMetricRegistry(metricRegistry);
        bootstrap.setMetricRegistry(metricRegistry);
        super.initialize(bootstrap);
    }

    @Override
    public void run(TimeLockServerConfiguration configuration, Environment environment) {
        TimeLockServer serverImpl = configuration.algorithm().createServerImpl(environment);
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
            TimeLockServer serverImpl) {
        Map<String, TimeLockServices> clientToServices = ImmutableMap.copyOf(Maps.asMap(
                configuration.clients(),
                client -> serverImpl.createInvalidatingTimeLockServices(client,
                        configuration.slowLockLogTriggerMillis())));

        for (Map.Entry<String, TimeLockServices> entry : clientToServices.entrySet()) {
            environment.getApplicationContext().addServlet(
                    new TimestampServletHolder(entry.getValue()), "/" + entry.getKey() + "/timestamp/fresh-timestamp");
        }
        environment.getApplicationContext().addServlet(
                new PaxosLeadershipServletHolder(clientToServices.values().iterator().next().getLeadershipAcceptor()),
                "/.internal/leaderPaxos/acceptor/latest-sequence-prepared-or-accepted");

        environment.jersey().register(HttpRemotingJerseyFeature.DEFAULT);
        environment.jersey().register(new TimeLockResource(clientToServices));
    }

    private static class TimestampServletHolder extends ServletHolder {
        TimestampServletHolder(TimeLockServices value) {
            super(new HttpServlet() {
                @Override
                protected void doPost(HttpServletRequest req, HttpServletResponse resp)
                        throws ServletException, IOException {
                    resp.addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
                    resp.getWriter().write(
                            new ObjectMapper().writeValueAsString(
                                    value.getTimestampService().getFreshTimestamp()
                            )
                    );
                }
            });
        }
    }

    private static class PaxosLeadershipServletHolder extends ServletHolder {
        PaxosLeadershipServletHolder(PaxosAcceptor acceptor) {
            super(new HttpServlet() {
                @Override
                protected void doPost(HttpServletRequest req, HttpServletResponse resp)
                        throws ServletException, IOException {
                    resp.addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
                    resp.getWriter().write(
                            new ObjectMapper().writeValueAsString(
                                    acceptor.getLatestSequencePreparedOrAccepted()
                            )
                    );
                }
            });
        }
    }
}
