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

import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import com.palantir.conjure.java.api.errors.QosException;
import com.palantir.leader.NotCurrentLeaderException;

/**
 * Converts {@link NotCurrentLeaderException} into appropriate status responses depending on the user's
 * {@link AtlasDbHttpProtocolVersion}. This is a 503 response in {@link AtlasDbHttpProtocolVersion#LEGACY_OR_UNKNOWN}.
 *
 * @author carrino
 */
public class NotCurrentLeaderExceptionMapper implements ExceptionMapper<NotCurrentLeaderException> {
    @Context
    private HttpHeaders httpHeaders;

    private static final HttpProtocolAwareExceptionTranslator<NotCurrentLeaderException> translator = new
            HttpProtocolAwareExceptionTranslator<>(new AtlasDbHttpProtocolHandler<NotCurrentLeaderException>() {
                @Override
                public Response handleLegacyOrUnknownVersion(NotCurrentLeaderException underlyingException) {
                    return ExceptionMappers.encode503ResponseWithRetryAfter(underlyingException);
                }

                @Override
                public QosException handleConjureJavaRuntime(NotCurrentLeaderException underlyingException) {
                    // TODO (jkong): Replace with 308s
                    return QosException.unavailable();
                }
            });

    @Override
    public Response toResponse(NotCurrentLeaderException exception) {
        return translator.translate(httpHeaders, exception);
    }
}
