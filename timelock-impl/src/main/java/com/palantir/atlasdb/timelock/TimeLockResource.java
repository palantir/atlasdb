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
package com.palantir.atlasdb.timelock;

import com.palantir.lock.LockService;
import com.palantir.logsafe.Safe;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * DO NOT add new endpoints in here. Instead, define them as Conjure endpoints.
 */
@Path("/{namespace: (?!(tl|lw)/)[a-zA-Z0-9_-]+}")
@Deprecated
public final class TimeLockResource {
    private final TimelockNamespaces namespaces;

    private TimeLockResource(TimelockNamespaces namespaces) {
        this.namespaces = namespaces;
    }

    public static TimeLockResource create(TimelockNamespaces namespaces) {
        return new TimeLockResource(namespaces);
    }

    @Path("/lock")
    public LockService getLockService(@Safe @PathParam("namespace") String namespace) {
        return namespaces.get(namespace).getLockService();
    }

    @Path("/timestamp")
    public TimestampService getTimeService(@Safe @PathParam("namespace") String namespace) {
        return namespaces.get(namespace).getTimestampService();
    }

    @Path("/timelock")
    public AsyncTimelockResource getTimelockService(@Safe @PathParam("namespace") String namespace) {
        return namespaces.get(namespace).getTimelockResource();
    }

    @Path("/timestamp-management")
    public TimestampManagementService getTimestampManagementService(@Safe @PathParam("namespace") String namespace) {
        return namespaces.getIgnoringDisabled(namespace).getTimestampManagementService();
    }
}
