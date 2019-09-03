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
import java.util.Optional;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.conjure.java.api.errors.QosException;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.logsafe.UnsafeArg;

/**
 * Converts {@link NotCurrentLeaderException} into appropriate status responses depending on the user's
 * {@link AtlasDbHttpProtocolVersion}. The intention is that clients should failover to other nodes, and there
 * is no need to wait before retrying as there is no indication that clients should do so.
 *
 * This is a 503 response in {@link AtlasDbHttpProtocolVersion#LEGACY_OR_UNKNOWN}.
 *
 * @author carrino
 */
public class NotCurrentLeaderExceptionMapper extends ProtocolAwareExceptionMapper<NotCurrentLeaderException> {
    private static final Logger log = LoggerFactory.getLogger(NotCurrentLeaderExceptionMapper.class);

    private final Optional<RedirectRetryTargeter> redirectRetryTargeter;

    @Context
    private UriInfo uriInfo;

    public NotCurrentLeaderExceptionMapper() {
        this.redirectRetryTargeter = Optional.empty();
    }

    public NotCurrentLeaderExceptionMapper(RedirectRetryTargeter redirectRetryTargeter) {
        this.redirectRetryTargeter = Optional.of(redirectRetryTargeter);
    }

    @Override
    Response handleLegacyOrUnknownVersion(NotCurrentLeaderException exception) {
        return ExceptionMappers.encode503ResponseWithoutRetryAfter(exception);
    }

    @Override
    QosException handleConjureJavaRuntime(NotCurrentLeaderException exception) {
        return redirectRetryTargeter.map(targeter -> {
            try {
                return QosException.retryOther(targeter.redirectRequest(uriInfo.getRequestUri().toURL()));
            } catch (MalformedURLException e) {
                log.warn("Unable to parse context information {} into a URL. Returning generic 503.",
                        UnsafeArg.of("uri", uriInfo.getRequestUri()), // may contain arbitrary query params
                        e);
                return QosException.unavailable();
            }
        }).orElseGet(QosException::unavailable);
    }
}
