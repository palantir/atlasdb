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
package com.palantir.atlasdb.timelock;

import java.util.Map;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import com.palantir.lock.LockService;
import com.palantir.timestamp.TimestampMigrationService;
import com.palantir.timestamp.TimestampService;

@Path("/{client: [a-zA-Z0-9_-]+}")
public class TimeLockResource {
    private final Map<String, TimeLockServices> clientToServices;

    public TimeLockResource(Map<String, TimeLockServices> clientToServices) {
        this.clientToServices = clientToServices;
    }

    @Path("/lock")
    public LockService getLockService(@PathParam("client") String client) {
        return getTimeLockServicesForClient(client).getLockService();
    }

    @Path("/timestamp")
    public TimestampService getTimeService(@PathParam("client") String client) {
        return getTimeLockServicesForClient(client).getTimestampService();
    }

    @Path("/timestamp-migration")
    public TimestampMigrationService getTimestampMigrationService(@PathParam("client") String client) {
        return getTimeLockServicesForClient(client).getTimestampMigrationService();
    }

    private TimeLockServices getTimeLockServicesForClient(String client) {
        TimeLockServices services = clientToServices.get(client);
        if (services == null) {
            throw new NotFoundException("Client doesn't exist");
        }
        return services;
    }
}
