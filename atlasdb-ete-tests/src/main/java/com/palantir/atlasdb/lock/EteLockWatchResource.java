/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.lock;

import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatchInfo;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.TimestampWithLockInfo;

@Path("lock-watch")
public interface EteLockWatchResource {
    @POST
    @Path("lock")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    LockToken lock(LockRequest request);

    @POST
    @Path("unlock")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    Set<LockToken> unlock(LockToken token);

    @POST
    @Path("register")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    void registerLockWatch(LockWatchReferences.LockWatchReference request);

    @POST
    @Path("start-transaction")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    long startTransaction();

    @POST
    @Path("watch-info")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    LockWatchInfo getLockWatchInfo(@QueryParam("startTs") long startTs, LockDescriptor lockDescriptor);

    @POST
    @Path("commit")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    TimestampWithLockInfo getCommitTimestampAndLockInfo(@QueryParam("startTs") long startTs, LockToken locksToIgnore);
}
