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
package com.palantir.timestamp;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

@Path("/timestamp-admin")
public interface TimestampAdminService {
    /**
     * Returns a timestamp that is greater than any timestamp already returned from the corresponding TimestampService.
     */
    @POST
    @Path("/upper-bound")
    long getUpperBoundTimestamp();

    /**
     * Fast forwards the timestamp to the specified one so that no one can be served fresh timestamps prior
     * to it from now on. It is assumed that no TimestampServices backed by the underlying store are currently
     * running.
     *
     * Furthermore, fast forwarding the timestamp is considered to be a valid means of re-validating an invalidated
     * timestamp admin service.
     *
     * @param newMinimumTimestamp The minimum timestamp to fast forward to.
     */
    @POST
    @Path("/fast-forward")
    void fastForwardTimestamp(@QueryParam("newMinimum") long newMinimumTimestamp);

    /**
     * Prevents the corresponding TimestampService from requesting additional timestamps.
     * It is assumed that no TimestampServices backed by the underlying store are currently running.
     */
    @POST
    @Path("/invalidate")
    void invalidateTimestamps();
}
