/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.annotations.VisibleForTesting;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.service.UserAgents;
import com.palantir.conjure.java.api.errors.QosException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.glassfish.jersey.spi.ExceptionMappers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class ProtocolAwareExceptionMapper<E extends Exception> implements ExceptionMapper<E> {
    private static final Logger log = LoggerFactory.getLogger(ProtocolAwareExceptionMapper.class);

    @Context
    private HttpHeaders httpHeaders;
    @Inject
    private Provider<ExceptionMappers> exceptionMappersProvider;

    @Override
    public Response toResponse(E exception) {
        AtlasDbHttpProtocolVersion version = getHttpProtocolVersion();
        switch (version) {
            case LEGACY_OR_UNKNOWN:
                return handleLegacyOrUnknownVersion(exception);
            case CONJURE_JAVA_RUNTIME:
                QosException qosException = handleConjureJavaRuntime(exception);
                return Optional.ofNullable(exceptionMappersProvider.get())
                        .map(provider -> provider.findMapping(qosException))
                        .map(mapper -> mapper.toResponse(qosException))
                        .orElseThrow(() -> new SafeIllegalStateException("Couldn't find QoS exception mapper."
                                + " This is a product bug."));
            default:
                log.warn("Couldn't determine what to do with protocol version {}. This is a product bug.",
                        SafeArg.of("protocolVersion", version));
                throw new SafeIllegalStateException("Unrecognized protocol version in HttpProtocolAwareExceptionMapper",
                        SafeArg.of("protocolVersion", version));
        }
    }

    private AtlasDbHttpProtocolVersion getHttpProtocolVersion() {
        return AtlasDbHttpProtocolVersion.inferFromString(
                Optional.ofNullable(httpHeaders.getRequestHeader(HttpHeaders.USER_AGENT))
                        .flatMap(ProtocolAwareExceptionMapper::parseProtocolVersionFromUserAgentHeader));
    }

    @VisibleForTesting
    static Optional<String> parseProtocolVersionFromUserAgentHeader(@Nonnull List<String> userAgentHeader) {
        return userAgentHeader
                .stream()
                .map(UserAgents::tryParse)
                .map(UserAgent::informational)
                .flatMap(List::stream)
                .filter(agent -> agent.name().equals(AtlasDbRemotingConstants.ATLASDB_HTTP_CLIENT))
                .findFirst()
                .map(UserAgent.Agent::version);
    }

    abstract Response handleLegacyOrUnknownVersion(E exception);
    abstract QosException handleConjureJavaRuntime(E exception);
}
