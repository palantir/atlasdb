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
import com.palantir.exception.NotInitializedException;
import javax.ws.rs.core.Response;

/**
 * Converts {@link NotInitializedException}s into appropriate status responses, depending on the user's
 * {@link AtlasDbHttpProtocolVersion}. The intention is that clients may retry on an arbitrary node of the service,
 * and they should backoff, as that node of the cluster is still starting up.
 *
 * This is a 503 without a {@code Retry-After} header and with a message body corresponding to
 * {@link NotInitializedException} in {@link AtlasDbHttpProtocolVersion#LEGACY_OR_UNKNOWN}.
 */
public class NotInitializedExceptionMapper extends ProtocolAwareExceptionMapper<NotInitializedException> {
    @Override
    Response handleLegacyOrUnknownVersion(NotInitializedException exception) {
        return ExceptionMappers.encode503ResponseWithoutRetryAfter(exception);
    }

    @Override
    QosException handleConjureJavaRuntime(NotInitializedException _exception) {
        return QosException.throttle();
    }
}
