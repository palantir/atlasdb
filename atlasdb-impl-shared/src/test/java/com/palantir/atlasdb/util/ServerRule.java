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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.conjure.java.server.jersey.ConjureJerseyFeature;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.conjure.java.undertow.runtime.ConjureHandler;
import com.palantir.conjure.java.undertow.runtime.ConjureUndertowRuntime;
import com.palantir.logsafe.Preconditions;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.server.HttpHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.ServletInfo;
import io.undertow.servlet.util.ImmediateInstanceFactory;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;
import javax.annotation.Nullable;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import org.glassfish.jersey.CommonProperties;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;
import org.junit.rules.ExternalResource;

public class ServerRule extends ExternalResource {
    private final Object[] resources;

    @Nullable
    private Undertow server;

    public ServerRule(Object... resources) {
        this.resources = resources;
    }

    public URI baseUri() {
        return URI.create("http://localhost:" + getLocalPort() + "/application");
    }

    private int getLocalPort() {
        InetSocketAddress address = (InetSocketAddress)
                Iterables.getOnlyElement(Preconditions.checkArgumentNotNull(server, "server has not been initialized")
                                .getListenerInfo())
                        .getAddress();
        return address.getPort();
    }

    @Override
    protected void before() throws Throwable {
        ResourceConfig jerseyConfig = new ResourceConfig()
                .property(CommonProperties.FEATURE_AUTO_DISCOVERY_DISABLE, true)
                .property(ServerProperties.WADL_FEATURE_DISABLE, true)
                .register(ConjureJerseyFeature.builder().build())
                .register(JacksonFeature.withoutExceptionMappers())
                .register(new ObjectMapperProvider(ObjectMappers.newServerJsonMapper()));

        for (Object object : resources) {
            if (object instanceof Class) {
                jerseyConfig.register((Class<?>) object);
            } else if (!(object instanceof UndertowService)) {
                jerseyConfig.register(object);
            }
        }

        ServletInfo jerseyServlet = new ServletInfo(
                "jersey", ServletContainer.class, new ImmediateInstanceFactory<>(new ServletContainer(jerseyConfig)));
        jerseyServlet.addMapping("/*");
        DeploymentInfo deployment = Servlets.deployment()
                .setClassLoader(getClass().getClassLoader())
                .setContextPath("/application")
                .setDeploymentName("servlet")
                .addServlet(jerseyServlet);
        DeploymentManager deploymentManager = Servlets.newContainer().addDeployment(deployment);
        deploymentManager.deploy();
        HttpHandler servletHandler = deploymentManager.start();

        HttpHandler conjureHandler = ConjureHandler.builder()
                .runtime(ConjureUndertowRuntime.builder().build())
                .addAllServices(Arrays.stream(resources)
                        .filter(UndertowService.class::isInstance)
                        .map(UndertowService.class::cast)
                        .collect(ImmutableList.toImmutableList()))
                .fallback(servletHandler)
                .build();

        server = Undertow.builder()
                .setServerOption(UndertowOptions.DECODE_URL, false)
                .addHttpListener(0, null, Handlers.path().addPrefixPath("/application", conjureHandler))
                .build();
        server.start();
    }

    @Provider
    public static final class ObjectMapperProvider implements ContextResolver<ObjectMapper> {

        private final ObjectMapper mapper;

        public ObjectMapperProvider(ObjectMapper mapper) {
            this.mapper = mapper;
        }

        @Override
        public ObjectMapper getContext(Class<?> _type) {
            return mapper;
        }
    }

    @Override
    protected void after() {
        Undertow serverSnapshot = server;
        server = null;
        if (serverSnapshot != null) {
            serverSnapshot.stop();
        }
    }
}
