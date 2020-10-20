/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.util;

import com.codahale.metrics.health.HealthCheck;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.server.SimpleServerFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.testing.DropwizardTestSupport;
import java.net.URI;
import org.junit.rules.ExternalResource;

/**
 * Copied from {@link io.dropwizard.testing.junit.DropwizardClientRule} so that we can configure the ObjectMapper.
 */
public class DropwizardClientRule extends ExternalResource {
    private final Object[] resources;
    private final DropwizardTestSupport<Configuration> testSupport;

    public DropwizardClientRule(Object... resources) {
        testSupport = new DropwizardTestSupport<Configuration>(FakeApplication.class, "") {
            @Override
            public Application<Configuration> newApplication() {
                return new FakeApplication();
            }
        };
        this.resources = resources;
    }

    public URI baseUri() {
        return URI.create("http://localhost:" + testSupport.getLocalPort() + "/application");
    }

    @Override
    protected void before() throws Throwable {
        testSupport.before();
    }

    @Override
    protected void after() {
        testSupport.after();
    }

    private static final class DummyHealthCheck extends HealthCheck {
        @Override
        protected HealthCheck.Result check() {
            return Result.healthy();
        }
    }

    private final class FakeApplication extends Application<Configuration> {

        @Override
        public void initialize(Bootstrap<Configuration> bootstrap) {
            bootstrap.getObjectMapper().registerModule(new Jdk8Module());
        }

        @Override
        public void run(Configuration configuration, Environment environment) {
            final SimpleServerFactory serverConfig = new SimpleServerFactory();
            configuration.setServerFactory(serverConfig);
            final HttpConnectorFactory connectorConfig = (HttpConnectorFactory) serverConfig.getConnector();
            connectorConfig.setPort(0);

            environment.healthChecks().register("dummy", new DummyHealthCheck());

            for (Object resource : resources) {
                if (resource instanceof Class<?>) {
                    environment.jersey().register((Class<?>) resource);
                } else {
                    environment.jersey().register(resource);
                }
            }
        }
    }
}
