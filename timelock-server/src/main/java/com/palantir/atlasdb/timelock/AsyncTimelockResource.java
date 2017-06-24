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

package com.palantir.atlasdb.timelock;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.v2.LockImmutableTimestampRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequestV2;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.timestamp.TimestampRange;

@Path("/timelock")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class AsyncTimelockResource {

    private final AsyncTimelockService timelock;

    public AsyncTimelockResource(AsyncTimelockService timelock) {
        this.timelock = timelock;
    }

    @POST
    @Path("fresh-timestamp")
    public long getFreshTimestamp() {
        return timelock.getFreshTimestamp();
    }

    @POST
    @Path("fresh-timestamps")
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return timelock.getFreshTimestamps(numTimestampsRequested);
    }

    @POST
    @Path("lock-immutable-timestamp")
    public LockImmutableTimestampResponse lockImmutableTimestamp(LockImmutableTimestampRequest request) {
        return timelock.lockImmutableTimestamp(request);
    }

    @POST
    @Path("immutable-timestamp")
    public long getImmutableTimestamp() {
        return timelock.getImmutableTimestamp();
    }

    @POST
    @Path("lock")
    public void lock(@Suspended final AsyncResponse response, LockRequestV2 request) {
        CompletableFuture<LockRefreshToken> future = timelock.lock(request);
        future.whenComplete((token, error) -> {
           if (error != null) {
               response.resume(error);
           } else {
               response.resume(token);
           }
        });
    }

    @POST
    @Path("await-locks")
    public void waitForLocks(@Suspended final AsyncResponse response, WaitForLocksRequest request) {
        CompletableFuture<Void> future = timelock.waitForLocks(request);
        future.whenComplete((result, error) -> {
            if (error != null) {
                response.resume(error);
            } else {
                response.resume(Response.noContent());
            }
        });
    }

    @POST
    @Path("refresh-locks")
    public Set<LockRefreshToken> refreshLockLeases(Set<LockRefreshToken> tokens) {
        return timelock.refreshLockLeases(tokens);
    }

    @POST
    @Path("unlock")
    public Set<LockRefreshToken> unlock(Set<LockRefreshToken> tokens) {
        return timelock.unlock(tokens);
    }

    @Path("current-time-millis")
    public long currentTimeMillis() {
        return timelock.currentTimeMillis();
    }
}
