/**
 * Copyright 2015 Palantir Technologies
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
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path("/timestamp")
public interface TimestampService {
    /**
     * This will get a fresh timestamp that is guaranteed to be newer than any other timestamp
     * handed out before this method was called.
     */
    @POST // This has to be POST because we can't allow caching.
    @Path("fresh-timestamp")
    @Produces(MediaType.APPLICATION_JSON)
    long getFreshTimestamp();

    /**
     * @return never null. Inclusive LongRange of timestamps.  Will always have at least 1 value.
     * This range may have less than the requested amount.
     */
    @POST // This has to be POST because we can't allow caching.
    @Path("fresh-timestamps")
    @Produces(MediaType.APPLICATION_JSON)
    TimestampRange getFreshTimestamps(@QueryParam("number") int numTimestampsRequested);

    /**
     * Fast forwards the timestamp to the specified one so that no one can be served fresh timestamps prior
     * to it from now on.
     *
     * Sets the upper limit in the TimestampBoundStore as well as increases the minimum timestamp that can
     * be allocated from this instantiation of the TimestampService moving forward.
     *
     * The caller of this is responsible for not using any of the fresh timestamps previously served to it,
     * and must call getFreshTimestamps() to ensure it is using timestamps after the fastforward point.
     *
     * @param newMinimumTimestamp
     */
    @POST // This has to be POST because we can't allow caching.
    @Path("fresh-timestamps")
    void fastForwardTimestamp(long newMinimumTimestamp);
}
