/**
 * Copyright 2017 Palantir Technologies
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

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public final class ExceptionMappers {
    private ExceptionMappers() {
        // utility
    }

    public static Response encode503ResponseWithoutRetryAfter(Exception exception) {
        return encode503ResponseInternal(exception).build();
    }

    public static Response encode503ResponseWithRetryAfter(Exception exception) {
        return encode503ResponseInternal(exception)
                .header(HttpHeaders.RETRY_AFTER, "0")
                .build();
    }

    private static Response.ResponseBuilder encode503ResponseInternal(Exception exception) {
        return Response.serverError()
                .entity(exception)
                .status(503)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
    }
}
