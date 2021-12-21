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
import com.palantir.lock.remoting.BlockingTimeoutException;
import java.time.Duration;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

/**
 * Converts {@link BlockingTimeoutException}s into appropriate status responses, depending on the user's
 * {@link AtlasDbHttpProtocolVersion}. The intention is that clients should retry on the same node (as in the absence
 * of exceptional circumstances, it would still be the leader), and they may do so immediately (as an individual lock
 * being locked does not imply that the server is struggling).
 *
 * This is a 503 response in {@link AtlasDbHttpProtocolVersion#LEGACY_OR_UNKNOWN}.
 */
public class BlockingTimeoutExceptionMapper extends ProtocolAwareExceptionMapper<BlockingTimeoutException> {
    @Override
    Response handleLegacyOrUnknownVersion(BlockingTimeoutException exception) {
        return Response.status(Status.SERVICE_UNAVAILABLE).build();
    }

    @Override
    QosException handleConjureJavaRuntime(BlockingTimeoutException exception) {
        return QosException.throttle(Duration.ZERO);
    }
}
