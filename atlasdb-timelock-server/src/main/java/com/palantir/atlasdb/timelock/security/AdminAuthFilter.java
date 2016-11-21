/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.timelock.security;

import java.io.IOException;
import java.util.List;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableList;

@Protected
public class AdminAuthFilter implements ContainerRequestFilter {
    private static final String API_KEY_HEADER = "api-key";
    private static final String SECRET = "secret"; // TODO replace with config secret. Should connect over HTTPS.
    private static final String UNAUTHORIZED_ERROR_MSG = "Unauthorized - please contact your Palantir administrator.";

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        if (requestBearsApiKey(requestContext)) {
            return; // pass
        }
        requestContext.abortWith(
                Response.status(Response.Status.UNAUTHORIZED)
                        .entity(UNAUTHORIZED_ERROR_MSG)
                        .build());
    }

    private boolean requestBearsApiKey(ContainerRequestContext requestContext) {
        List<String> headers = requestContext.getHeaders().getOrDefault(API_KEY_HEADER, ImmutableList.of());
        return headers.contains(SECRET);
    }
}
