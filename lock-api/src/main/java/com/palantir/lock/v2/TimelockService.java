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

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockRefreshToken;
import com.palantir.timestamp.TimestampRange;

@Path("/timelock")
public interface TimelockService {

    @POST
    @Path("fresh-timestamp")
    @Produces(MediaType.APPLICATION_JSON)
    long getFreshTimestamp();

    @POST
    @Path("fresh-timestamps")
    @Produces(MediaType.APPLICATION_JSON)
    TimestampRange getFreshTimestamps(@QueryParam("number") int numTimestampsRequested);

    @POST
    @Path("lock-immutable-timestamp")
    @Produces(MediaType.APPLICATION_JSON)
    LockImmutableTimestampResponse lockImmutableTimestamp();

    @POST
    @Path("immutable-timestamp")
    @Produces(MediaType.APPLICATION_JSON)
    long getImmutableTimestamp();

    @POST
    @Path("lock")
    @Produces(MediaType.APPLICATION_JSON)
    LockRefreshToken lock(LockRequestV2 request);

    @POST
    @Path("await-locks")
    @Produces(MediaType.APPLICATION_JSON)
    void waitForLocks(Set<LockDescriptor> lockDescriptors);

    @POST
    @Path("refresh-locks")
    @Produces(MediaType.APPLICATION_JSON)
    Set<LockRefreshToken> refreshLockLeases(Set<LockRefreshToken> tokens);

    @POST
    @Path("unlock")
    @Produces(MediaType.APPLICATION_JSON)
    Set<LockRefreshToken> unlock(Set<LockRefreshToken> tokens);

    @POST
    @Path("current-time-millis")
    @Produces(MediaType.APPLICATION_JSON)
    long currentTimeMillis();

}
