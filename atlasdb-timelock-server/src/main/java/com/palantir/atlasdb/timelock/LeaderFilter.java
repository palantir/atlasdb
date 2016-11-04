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
package com.palantir.atlasdb.timelock;

import java.io.IOException;
import java.util.Objects;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;

import com.google.common.util.concurrent.Futures;

import io.atomix.variables.DistributedValue;

public class LeaderFilter implements ContainerRequestFilter {
    private final String localId;
    private final DistributedValue<String> leaderId;

    public LeaderFilter(String localId, DistributedValue<String> leaderId) {
        this.localId = localId;
        this.leaderId = leaderId;
    }

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        if (!Objects.equals(localId, Futures.getUnchecked(leaderId.get()))) {
            requestContext.abortWith(Response
                    .status(503)
                    .header("Retry-After", "0")
                    .build());
        }
    }
}
