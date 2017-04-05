/*
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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import com.google.common.base.Supplier;
import com.palantir.lock.LockService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;

@Path("/{client: [a-zA-Z0-9_-]+}")
public class TimeLockResource {
    private final Supplier<Set<String>> clients;
    private final MyMethodType<TimeLockServices> create;
    private Map<String, TimeLockServices> clientToServices = new HashMap<>();

    public TimeLockResource(Supplier<Set<String>> clients, MyMethodType<TimeLockServices> create) {
        this.clients = clients;
        this.create = create;
    }

    @Path("/lock")
    public LockService getLockService(@PathParam("client") String client) {
        return getTimeLockServicesForClient(client).getLockService();
    }

    @Path("/timestamp")
    public TimestampService getTimeService(@PathParam("client") String client) {
        return getTimeLockServicesForClient(client).getTimestampService();
    }

    @Path("/timestamp-management")
    public TimestampManagementService getTimestampManagementService(@PathParam("client") String client) {
        return getTimeLockServicesForClient(client).getTimestampManagementService();
    }

    private TimeLockServices getTimeLockServicesForClient(String client) {
        TimeLockServices services = clientToServices.get(client);
        if (services == null) {
            if (clients.get().contains(client)) {
                services = create.call(client);
                clientToServices.put(client, services);
            } else {
                throw new NotFoundException("Client doesn't exist");
            }
        }
        return services;
    }

    interface MyMethodType<TimelockServices> {
        TimelockServices call(String param);
    }
}
