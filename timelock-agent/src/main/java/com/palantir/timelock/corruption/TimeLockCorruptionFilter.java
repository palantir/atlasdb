/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.corruption;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.Response;

import com.palantir.atlasdb.timelock.corruption.TimeLockCorruptionHealthCheck;

@PreMatching
public class TimeLockCorruptionFilter implements ContainerRequestFilter {
    private TimeLockCorruptionHealthCheck healthCheck;

    public TimeLockCorruptionFilter(TimeLockCorruptionHealthCheck healthCheck) {
        this.healthCheck = healthCheck;
    }

    @Override
    public void filter(ContainerRequestContext requestContext) {
        if (!healthCheck.isHealthy()) {
            requestContext.abortWith(Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity("TimeLock is not available on account of data corruption.").build());
        }
    }
}

