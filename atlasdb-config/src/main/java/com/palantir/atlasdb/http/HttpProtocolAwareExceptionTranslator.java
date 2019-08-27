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

import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.spi.ExceptionMappers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.conjure.java.api.errors.QosException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

public class HttpProtocolAwareExceptionTranslator<E extends Exception> {
    private static final Logger log = LoggerFactory.getLogger(HttpProtocolAwareExceptionTranslator.class);

    private final AtlasDbHttpProtocolHandler<E> httpProtocolHandler;

    @Inject
    private Provider<ExceptionMappers> exceptionMappersProvider;

    public HttpProtocolAwareExceptionTranslator(AtlasDbHttpProtocolHandler<E> httpProtocolHandler) {
        this.httpProtocolHandler = httpProtocolHandler;
    }

    public Response translate(HttpHeaders httpHeaders, E exception) {
        AtlasDbHttpProtocolVersion protocolVersion = AtlasDbHttpProtocolVersion.inferFromHttpHeaders(httpHeaders);

        switch (protocolVersion) {
            case LEGACY_OR_UNKNOWN:
                return httpProtocolHandler.handleLegacyOrUnknownVersion(exception);
            case CONJURE_JAVA_RUNTIME:
                QosException qosException = httpProtocolHandler.handleConjureJavaRuntime(exception);
                return exceptionMappersProvider.get().findMapping(qosException).toResponse(qosException);
            default:
                log.warn("Couldn't determine what to do with protocol version {}. This is a product bug.",
                        SafeArg.of("protocolVersion", protocolVersion));
                throw new SafeIllegalStateException("Unrecognized protocol version in HttpProtocolAwareExceptionMapper",
                        SafeArg.of("protocolVersion", protocolVersion));
        }
    }
}
