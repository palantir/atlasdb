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
package com.palantir.atlasdb.http;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.ExceptionMapper;

import com.palantir.conjure.java.api.errors.QosException;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

/**
 * Converts {@link NotCurrentLeaderException} into appropriate status responses depending on the user's
 * {@link AtlasDbHttpProtocolVersion}. The intention is that clients should failover to other nodes, and there
 * is no need to wait before retrying as there is no indication that clients should do so.
 *
 * This is a 503 response in {@link AtlasDbHttpProtocolVersion#LEGACY_OR_UNKNOWN}.
 *
 * @author carrino
 */
public class NotCurrentLeaderExceptionMapper implements ExceptionMapper<NotCurrentLeaderException> {
    @Context
    private HttpHeaders httpHeaders;

    @Context
    private UriInfo uriInfo;

    @Inject
    private Provider<org.glassfish.jersey.spi.ExceptionMappers> exceptionMappersProvider;

    private final Optional<List<URL>> servers;
    private final HttpProtocolAwareExceptionTranslator<NotCurrentLeaderException> translator;

    public NotCurrentLeaderExceptionMapper() {
        this(Optional.empty());
    }

    public NotCurrentLeaderExceptionMapper(Optional<List<String>> serverNames) {
        this.servers = serverNames.map(
                presentServerStrings -> presentServerStrings.stream()
                        .map(string -> {
                            try {
                                return new URL("https://" + string);
                            } catch (MalformedURLException e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .collect(Collectors.toList()));
        this.translator = new
                HttpProtocolAwareExceptionTranslator<>(
                AtlasDbHttpProtocolHandler.LambdaHandler.of(
                        ExceptionMappers::encode503ResponseWithRetryAfter,
                        $ -> {
                            if (this.servers.isPresent()) {
                                List<URL> snapshot = this.servers.get();
                                URL randomServer = snapshot.get(ThreadLocalRandom.current().nextInt(snapshot.size()));
                                try {
                                    URL requestedUrl = uriInfo.getAbsolutePath().toURL();
                                    return QosException.retryOther(
                                            new URL(requestedUrl.getProtocol(),
                                                    randomServer.getHost(),
                                                    randomServer.getPort(),
                                                    requestedUrl.getFile()));
                                } catch (MalformedURLException e) {
                                    throw new SafeIllegalStateException("Error in TimeLock's redirection handler",
                                            e);
                                }
                            }
                            return QosException.unavailable();
                        }));
    }

    @Override
    public Response toResponse(NotCurrentLeaderException exception) {
        return translator.translate(exceptionMappersProvider, httpHeaders, exception);
    }
}
