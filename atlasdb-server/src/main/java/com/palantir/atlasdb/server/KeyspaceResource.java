/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.server;

import javax.ws.rs.Path;

import com.palantir.atlasdb.factory.TransactionManagers.LockAndTimestampServices;
import com.palantir.lock.RemoteLockService;
import com.palantir.timestamp.TimestampService;

public class KeyspaceResource {
    private final LockAndTimestampServices services;

    public KeyspaceResource(LockAndTimestampServices services) {
        this.services = services;
    }

    @Path("/lock")
    public RemoteLockService getLockService() {
        return services.lock();
    }

    @Path("/timestamp")
    public TimestampService getTimestampService() {
        return services.time();
    }
}
