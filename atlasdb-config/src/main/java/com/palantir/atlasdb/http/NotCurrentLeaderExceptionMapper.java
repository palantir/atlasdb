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

<<<<<<< HEAD
import java.time.Duration;
import java.util.Optional;

=======
import java.util.Optional;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
>>>>>>> ee06686... Parse headers if needed
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.http.negotiation.AtlasDbHttpProtocolVersion;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

/**
 * Convert {@link NotCurrentLeaderException} into a 503 status response.
 *
 * @author carrino
 */
public class NotCurrentLeaderExceptionMapper implements ExceptionMapper<NotCurrentLeaderException> {
    private static final Logger log = LoggerFactory.getLogger(NotCurrentLeaderExceptionMapper.class);

    @Context
    private HttpHeaders httpHeaders;

    /**
     * Returns a response equal to a response when encountering a
     * {@link com.palantir.conjure.java.api.errors.QosException.Unavailable} exception.
     */
    @Override
    public Response toResponse(NotCurrentLeaderException exception) {
        AtlasDbHttpProtocolVersion protocolVersion = parseProtocolVersion(httpHeaders);
        switch (protocolVersion) {
            case LEGACY_ATLASDB_FEIGN:
                return ExceptionMappers.encode503ResponseWithRetryAfter(exception);
            case CONJURE_JAVA_RUNTIME:
                // TODO (jkong): Implement this case. CJR is resilient to our old behaviour, just that it deals
                // with it inefficiently, so this is acceptable for now.
                return ExceptionMappers.encode503ResponseWithRetryAfter(exception);
            default:
                log.warn("Couldn't determine what to do with protocol version {}. This is a product bug.",
                        SafeArg.of("protocolVersion", protocolVersion));
                throw new SafeIllegalStateException("Unrecognized protocol version in NotCurrentLeaderExceptionMapper",
                        SafeArg.of("protocolVersion", protocolVersion));
        }
    }

    private static AtlasDbHttpProtocolVersion parseProtocolVersion(HttpHeaders headers) {
        String httpProtocolVersion = headers.getHeaderString(AtlasDbHttpProtocolVersion.VERSION_HEADER);
        return Optional.ofNullable(httpProtocolVersion)
                .flatMap(AtlasDbHttpProtocolVersion::fromStringRepresentation)
                .orElse(AtlasDbHttpProtocolVersion.LEGACY_ATLASDB_FEIGN);
    }
}
