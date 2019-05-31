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

import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import com.palantir.atlasdb.timelock.auth.AuthManager;
import com.palantir.atlasdb.timelock.auth.api.ClientId;
import com.palantir.atlasdb.timelock.auth.api.Password;
import com.palantir.lock.TimelockNamespace;
import com.palantir.logsafe.Safe;

@Path("/")
public class AuthorizedTimeLockResource {
    private final TimeLockResource timeLockResource;
    private final AuthManager authManager;

    private AuthorizedTimeLockResource(TimeLockResource timeLockResource, AuthManager authManager) {
        this.timeLockResource = timeLockResource;
        this.authManager = authManager;
    }

    public static AuthorizedTimeLockResource create(TimeLockResource timeLockResource, AuthManager authManager) {
        return new AuthorizedTimeLockResource(timeLockResource, authManager);
    }

    @Path("/{namespace: [a-zA-Z0-9_-]+}")
    public TimeLockResource getTimeLockResource(
            @Safe @PathParam("namespace") TimelockNamespace namespace,
            @HeaderParam("client-id") ClientId clientId,
            @HeaderParam("password") Password password) {
        authManager.checkAuthorized(clientId, password, namespace);
        return timeLockResource;
    }

    public int getNumberOfActiveClients() {
        return timeLockResource.getNumberOfActiveClients();
    }

    public int getMaxNumberOfClients() {
        return timeLockResource.getMaxNumberOfClients();
    }
}
