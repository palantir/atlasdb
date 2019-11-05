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

package com.palantir.atlasdb.timelock.lock;

import java.util.Map;
import java.util.UUID;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockWatch;
import com.palantir.lock.watch.LockWatchRequest;

@Path("/lock-watch")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class LockWatchingResource {
    private final LockWatchingService lockWatchingService;

    public LockWatchingResource(LockWatchingService lockWatchingService) {
        this.lockWatchingService = lockWatchingService;
    }

    @POST
    @Path("start-watching")
    void startWatching(LockWatchRequest lockWatchRequest) {
        lockWatchingService.startWatching(lockWatchRequest.serviceId(), lockWatchRequest.lockDescriptors());
    }

    @POST
    @Path("stop-watching")
    void stopWatching(LockWatchRequest lockWatchRequest) {
        lockWatchingService.stopWatching(lockWatchRequest.serviceId(), lockWatchRequest.lockDescriptors());
    }

    @POST
    @Path("get-watch-state")
    Map<LockDescriptor, LockWatch> getWatchState(UUID serviceId) {
        return lockWatchingService.getWatchState(serviceId);
    }
}
