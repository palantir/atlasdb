/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.HttpHeaders;

public class SmartLeaderFilter implements ContainerResponseFilter {
    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
            throws IOException {
        if (responseContext.getStatus() == 503) {
            if (responseContext.getHeaderString("XXX-LeaderHint") != null) {
                // do magic
                String newLeader = requestContext.getHeaderString("XXX-LeaderHint");
                URI originalPath = requestContext.getUriInfo().getAbsolutePath();
                responseContext.setStatus(308);
                try {
                    responseContext.getStringHeaders().add(HttpHeaders.LOCATION,
                            new URI(originalPath.getScheme(),
                                    newLeader,
                                    originalPath.getPath(),
                                    originalPath.getFragment()).toString());
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
