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

import com.palantir.conjure.java.api.errors.QosException;
import com.palantir.leader.NotCurrentLeaderException;
import java.util.Optional;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

/**
 * Converts {@link NotCurrentLeaderException} into appropriate status responses depending on the user's
 * {@link AtlasDbHttpProtocolVersion}. The intention is that clients should failover to other nodes, and there
 * is no need to wait before retrying as there is no indication that clients should do so.
 *
 * This is a 503 response in {@link AtlasDbHttpProtocolVersion#LEGACY_OR_UNKNOWN}.
 */
public class NotCurrentLeaderExceptionMapper extends ProtocolAwareExceptionMapper<NotCurrentLeaderException> {
    private final Optional<RedirectRetryTargeter> redirectRetryTargeter;

    public NotCurrentLeaderExceptionMapper() {
        this.redirectRetryTargeter = Optional.empty();
    }

    public NotCurrentLeaderExceptionMapper(RedirectRetryTargeter redirectRetryTargeter) {
        this.redirectRetryTargeter = Optional.of(redirectRetryTargeter);
    }

    @Override
    Response handleLegacyOrUnknownVersion(NotCurrentLeaderException exception) {
        return Response.status(Status.SERVICE_UNAVAILABLE).build();
    }

    @Override
    QosException handleConjureJavaRuntime(NotCurrentLeaderException exception) {
        return redirectRetryTargeter
                .flatMap(redirectTargeter -> redirectTargeter.redirectRequest(exception.getServiceHint()))
                .<QosException>map(QosException::retryOther)
                .orElseGet(QosException::unavailable);
    }
}
