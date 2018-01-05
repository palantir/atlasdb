/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.http;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import com.palantir.atlasdb.qos.ratelimit.RateLimitExceededException;

/**
 * This class maps instances of {@link RateLimitExceededException} to HTTP responses with a 429 status code.
 * Users may register this exception mapper for a standard way of converting these exceptions to 429 responses, which
 * are meaningful in that {@link com.palantir.remoting3.jaxrs.JaxRsClient}s are able to back off accordingly.
 */
public class RateLimitExceededExceptionMapper implements ExceptionMapper<RateLimitExceededException> {
    @Override
    public Response toResponse(RateLimitExceededException exception) {
        return ExceptionMappers.encodeExceptionResponse(exception, 429).build();
    }
}
