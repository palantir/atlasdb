/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.timestamp;

import com.palantir.logsafe.Safe;
import com.palantir.processors.AutoDelegate;
import com.palantir.processors.DoDelegate;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

/**
 * A {@link TimestampService} serves a sequence of timestamps. Requests to {@link TimestampService#getFreshTimestamp()}
 * and {@link TimestampService#getFreshTimestamps(int)} should return results that are unique and increasing, in that
 * requests to each method must return timestamps greater than any timestamps observable before a request was
 * initiated.
 *
 * Where used in the context of AtlasDB, it is expected that timestamp services begin their sequences at or above
 * AtlasDbConstants#STARTING_TS (1). Failure to do so may result in unexpected behaviour.
 */
@Path("/timestamp")
@AutoDelegate
public interface TimestampService {
    /**
     * Used for TimestampServices that can be initialized asynchronously; other TimestampServices can keep the default
     * implementation, and return true (they're trivially fully initialized).
     *
     * @return true iff the TimestampService has been fully initialized and is ready to use
     */
    @DoDelegate
    default boolean isInitialized() {
        return true;
    }

    /**
     * A request to this method should return a timestamp that is globally unique, and greater than any timestamp that
     * may have been observed before the request was initiated.
     */
    @POST // This has to be POST because we can't allow caching.
    @Path("fresh-timestamp")
    @Produces(MediaType.APPLICATION_JSON)
    long getFreshTimestamp();

    /**
     * A request to this method should return a {@link TimestampRange}, where all of the timestamps are globally unique
     * and greater than any timestamp that may have been observed before the request was initiated.
     *
     * @return never null; inclusive LongRange of timestamps that always has at least 1 value
     * This range may have less than the requested amount.
     */
    @POST // This has to be POST because we can't allow caching.
    @Path("fresh-timestamps")
    @Produces(MediaType.APPLICATION_JSON)
    TimestampRange getFreshTimestamps(@Safe @QueryParam("number") int numTimestampsRequested);
}
