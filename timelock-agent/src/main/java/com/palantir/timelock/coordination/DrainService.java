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

package com.palantir.timelock.coordination;

import java.util.function.Supplier;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.palantir.atlasdb.timelock.TimeLockServices;

/**
 * Shuts timestamp and lock services down for a single client.
 * This does:
 *   - close the lock service
 *   - drain the leader election service
 *   - drain the timestamp service
 * This method *BLOCKS* until all outstanding requests for the given client have completed.
 */
@Path("/drain")
public interface DrainService {
    void register(String client, Supplier<TimeLockServices> timeLockServices);

    @POST
    @Path("drain")
    @Produces(MediaType.APPLICATION_JSON)
    void drain(@QueryParam("client") String client);

    @POST
    @Path("regenerate")
    @Produces(MediaType.APPLICATION_JSON)
    default void regenerate(@QueryParam("client") String client) {
        regenerateInternal(client);
    }

    TimeLockServices regenerateInternal(String client);
}
