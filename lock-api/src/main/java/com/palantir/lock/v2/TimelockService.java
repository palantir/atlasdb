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

import com.palantir.logsafe.Safe;
import com.palantir.timestamp.TimestampRange;

@Path("/timelock")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface TimelockService {
    /**
     * Used for TimelockServices that can be initialized asynchronously (i.e. those extending
     * {@link com.palantir.async.initializer.AsyncInitializer}; other TimelockServices can keep the default
     * implementation, and return true (they're trivially fully initialized).
     *
     * @return true iff the TimelockService has been fully initialized and is ready to use
     */
    default boolean isInitialized() {
        return true;
    }

    @POST
    @Path("fresh-timestamp")
    long getFreshTimestamp();

    @POST
    @Path("fresh-timestamps")
    TimestampRange getFreshTimestamps(@Safe @QueryParam("number") int numTimestampsRequested);

    @POST
    @Path("lock-immutable-timestamp")
    LockImmutableTimestampResponse lockImmutableTimestamp(LockImmutableTimestampRequest request);

    @POST
    @Path("immutable-timestamp")
    long getImmutableTimestamp();

    @POST
    @Path("lock")
    LockResponse lock(LockRequest request);

    @POST
    @Path("await-locks")
    WaitForLocksResponse waitForLocks(WaitForLocksRequest request);

    @POST
    @Path("refresh-locks")
    Set<LockToken> refreshLockLeases(Set<LockToken> tokens);

    /**
     * Releases locks associated with the set of {@link LockToken}s provided.
     * The set of tokens returned are the tokens for which the associated locks were unlocked in this call.
     * It is possible that a token that was provided is NOT in the returned set (e.g. if it expired).
     * However, in this case it is guaranteed that that token is no longer valid.
     *
     * @param tokens Tokens for which associated locks should be unlocked.
     * @return Tokens for which associated locks were unlocked.
     */
    @POST
    @Path("unlock")
    Set<LockToken> unlock(Set<LockToken> tokens);

    /**
     * A version of {@link TimelockService#unlock(Set)} where one does not need to know whether the locks associated
     * with the provided tokens were successfully unlocked or not.
     *
     * In some implementations, this may be more performant than a standard unlock.
     *
     * @param tokens Tokens for which associated locks should be unlocked.
     */
    default void tryUnlock(Set<LockToken> tokens) {
        unlock(tokens);
    }

    @POST
    @Path("current-time-millis")
    long currentTimeMillis();

}
