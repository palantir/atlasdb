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

package com.palantir.lock.v2;

import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.palantir.timestamp.TimestampRange;

@Path("/timelock")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface TimelockService {

    @POST
    @Path("fresh-timestamp")
    long getFreshTimestamp();

    @POST
    @Path("fresh-timestamps")
    TimestampRange getFreshTimestamps(@QueryParam("number") int numTimestampsRequested);

    @POST
    @Path("lock-immutable-timestamp")
    LockImmutableTimestampResponse lockImmutableTimestamp(LockImmutableTimestampRequest request);

    @POST
    @Path("immutable-timestamp")
    long getImmutableTimestamp();

    @POST
    @Path("lock")
    LockTokenV2 lock(LockRequestV2 request);

    @POST
    @Path("await-locks")
    void waitForLocks(WaitForLocksRequest request);

    @POST
    @Path("refresh-locks")
    Set<LockTokenV2> refreshLockLeases(Set<LockTokenV2> tokens);

    @POST
    @Path("unlock")
    Set<LockTokenV2> unlock(Set<LockTokenV2> tokens);

    @POST
    @Path("current-time-millis")
    long currentTimeMillis();

}
