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

package com.palantir.atlasdb.timelock;

import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.palantir.tokens.auth.AuthHeader;

@Path("/secure-timelock")
public interface SecureTimelockService {

    /**
     * A request to this method should return a timestamp greater than any timestamp
     * that may have been observed before the request was initiated.
     */
    @POST // This has to be POST because we can't allow caching.
    @Path("fresh-timestamp")
    @Produces(MediaType.APPLICATION_JSON)
    long getFreshTimestamp(@HeaderParam("auth") AuthHeader authHeader);

}
