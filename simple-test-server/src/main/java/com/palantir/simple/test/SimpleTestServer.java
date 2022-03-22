/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.simple.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.conjure.java.server.jersey.ConjureJerseyFeature;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.conjure.java.undertow.runtime.ConjureHandler;
import com.palantir.conjure.java.undertow.runtime.ConjureUndertowRuntime;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.Undertow.ListenerBuilder;
import io.undertow.Undertow.ListenerType;
import io.undertow.UndertowOptions;
import io.undertow.server.HttpHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.ServletInfo;
import io.undertow.servlet.util.ImmediateInstanceFactory;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.servlet.ServletException;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import org.glassfish.jersey.CommonProperties;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;

public final class SimpleTestServer {
    private final List<? extends Consumer<? super ResourceConfig>> jerseyComponents;
    private final List<? extends UndertowService> undertowServices;
    private final String contextPath;
    private final OptionalInt port;
    private final Optional<SSLContext> sslContext;

    @Nullable
    private Undertow server;

    private SimpleTestServer(
            String contextPath,
            OptionalInt port,
            Optional<SSLContext> sslContext,
            List<? extends Consumer<? super ResourceConfig>> jerseyComponents,
            List<? extends UndertowService> undertowServices) {
        Preconditions.checkArgument(
                !jerseyComponents.isEmpty() || !undertowServices.isEmpty(), "No API components have been provided");
        this.contextPath = contextPath;
        this.port = port;
        this.sslContext = sslContext;
        this.jerseyComponents = jerseyComponents;
        this.undertowServices = undertowServices;
    }

    public void start() {
        Preconditions.checkState(server == null, "Server is already running");
        ResourceConfig jerseyConfig = new ResourceConfig()
                .property(CommonProperties.FEATURE_AUTO_DISCOVERY_DISABLE, true)
                .property(ServerProperties.WADL_FEATURE_DISABLE, true)
                .register(ConjureJerseyFeature.builder().build())
                .register(JacksonFeature.withoutExceptionMappers())
                .register(new ObjectMapperProvider(ObjectMappers.newServerJsonMapper()));

        jerseyComponents.forEach(item -> item.accept(jerseyConfig));

        ServletInfo jerseyServlet = new ServletInfo(
                "jersey", ServletContainer.class, new ImmediateInstanceFactory<>(new ServletContainer(jerseyConfig)));
        jerseyServlet.addMapping("/*");
        DeploymentInfo deployment = Servlets.deployment()
                .setClassLoader(getClass().getClassLoader())
                .setContextPath(contextPath)
                .setDeploymentName("servlet")
                .addServlet(jerseyServlet);
        DeploymentManager deploymentManager = Servlets.newContainer().addDeployment(deployment);
        deploymentManager.deploy();
        HttpHandler servletHandler;
        try {
            servletHandler = deploymentManager.start();
        } catch (ServletException e) {
            throw new SafeRuntimeException("Failed to start servlets", e);
        }

        HttpHandler conjureHandler = ConjureHandler.builder()
                .runtime(ConjureUndertowRuntime.builder().build())
                .addAllServices(undertowServices)
                .fallback(servletHandler)
                .build();

        server = Undertow.builder()
                .setServerOption(UndertowOptions.DECODE_URL, false)
                .setServerOption(UndertowOptions.ALLOW_ENCODED_SLASH, true)
                .setServerOption(UndertowOptions.SHUTDOWN_TIMEOUT, 500)
                .addListener(sslContext
                        .map(ctx -> new ListenerBuilder()
                                .setType(ListenerType.HTTPS)
                                .setSslContext(ctx))
                        .orElseGet(() -> new ListenerBuilder().setType(ListenerType.HTTP))
                        .setPort(port.orElse(0))
                        .setHost("0.0.0.0")
                        .setRootHandler(Handlers.path().addPrefixPath(contextPath, conjureHandler)))
                .build();
        server.start();
    }

    public void stop() {
        Undertow serverSnapshot = server;
        server = null;
        if (serverSnapshot != null) {
            serverSnapshot.stop();
        }
    }

    public int getPort() {
        InetSocketAddress address = (InetSocketAddress)
                Iterables.getOnlyElement(Preconditions.checkArgumentNotNull(server, "server has not been initialized")
                                .getListenerInfo())
                        .getAddress();
        return address.getPort();
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

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final List<Consumer<? super ResourceConfig>> jerseyComponents = new ArrayList<>();
        private final List<UndertowService> undertowServices = new ArrayList<>();

        private String contextPath = "/";
        private OptionalInt port = OptionalInt.empty();
        private Optional<SSLContext> sslContext = Optional.empty();

        private Builder() {}

        public Builder jersey(Consumer<? super ResourceConfig> value) {
            jerseyComponents.add(Preconditions.checkNotNull(value, "Jersey Component"));
            return this;
        }

        public Builder undertow(UndertowService value) {
            undertowServices.add(Preconditions.checkNotNull(value, "Undertow Service"));
            return this;
        }

        public Builder contextPath(String value) {
            contextPath = Preconditions.checkNotNull(value, "context path");
            return this;
        }

        public Builder port(int value) {
            port = OptionalInt.of(value);
            return this;
        }

        public Builder sslContext(SSLContext value) {
            sslContext = Optional.of(Preconditions.checkNotNull(value, "SSLContext"));
            return this;
        }

        public SimpleTestServer build() {
            return new SimpleTestServer(
                    contextPath,
                    port,
                    sslContext,
                    ImmutableList.copyOf(jerseyComponents),
                    ImmutableList.copyOf(undertowServices));
        }
    }
}
