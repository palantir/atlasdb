/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.auth;

import java.io.IOException;
import java.util.Map;

import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import io.dropwizard.auth.AuthFilter;

@PreMatching
@Priority(Priorities.AUTHENTICATION)
public class TimelockAuthFilter extends AuthFilter {

    private final Map<String, String> clientTokens;
    private final String adminToken = "admin";

    public TimelockAuthFilter(Map<String, String> clientTokens) {
        this.clientTokens = clientTokens;
    }

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        String namespace = requestContext.getUriInfo().getPathParameters().getFirst("namespace");
        String providedToken = requestContext.getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
        if (clientTokens.containsKey(namespace) && !clientTokens.get(namespace).equals(providedToken)
                && !providedToken.equals(adminToken)) {
            throw new WebApplicationException(Response.Status.UNAUTHORIZED);
        }
    }
}
