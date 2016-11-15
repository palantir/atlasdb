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

import com.palantir.timestamp.TimestampAdministrationService;

@Path("/{client: [a-zA-Z0-9_-]+}")
public class TimestampAdministrationResource {
    private final Map<String, TimestampAdministrationService> clientToServices;

    public TimestampAdministrationResource(Map<String, TimestampAdministrationService> clientToServices) {
        this.clientToServices = clientToServices;
    }

    @Path("/timestamp-administration")
    public TimestampAdministrationService getTimestampAdministrationService(@PathParam("client") String client) {
        TimestampAdministrationService service = clientToServices.get(client);
        if (service == null) {
            throw new NotFoundException("Client doesn't exist");
        }
        return service;
    }
}
