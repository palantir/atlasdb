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

import com.palantir.conjure.java.api.errors.QosException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.glassfish.jersey.spi.ExceptionMappers;

abstract class ProtocolAwareExceptionMapper<E extends Exception> implements ExceptionMapper<E> {
    private static final SafeLogger log = SafeLoggerFactory.get(ProtocolAwareExceptionMapper.class);

    @Context
    private HttpHeaders httpHeaders;

    @Inject
    private Provider<ExceptionMappers> exceptionMappersProvider;

    @Override
    public Response toResponse(E exception) {
        AtlasDbHttpProtocolVersion version =
                ProtocolAwareness.getHttpProtocolVersion(httpHeaders.getRequestHeader(HttpHeaders.USER_AGENT));
        switch (version) {
            case LEGACY_OR_UNKNOWN:
                return handleLegacyOrUnknownVersion(exception);
            case CONJURE_JAVA_RUNTIME:
                QosException qosException = handleConjureJavaRuntime(exception);
                return Optional.ofNullable(exceptionMappersProvider.get())
                        .map(provider -> provider.findMapping(qosException))
                        .map(mapper -> mapper.toResponse(qosException))
                        .orElseThrow(() -> new SafeIllegalStateException(
                                "Couldn't find QoS exception mapper." + " This is a product bug."));
            default:
                log.warn(
                        "Couldn't determine what to do with protocol version {}. This is a product bug.",
                        SafeArg.of("protocolVersion", version));
                throw new SafeIllegalStateException(
                        "Unrecognized protocol version in HttpProtocolAwareExceptionMapper",
                        SafeArg.of("protocolVersion", version));
        }
    }

    abstract Response handleLegacyOrUnknownVersion(E exception);

    abstract QosException handleConjureJavaRuntime(E exception);
}
