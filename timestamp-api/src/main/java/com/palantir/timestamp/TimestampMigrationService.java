/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.timestamp;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path("/timestamp-migration")
public interface TimestampMigrationService {
    /**
     * Updates the timestamp service to the currentTimestamp to ensure that all fresh timestamps issued after
     * this request are greater than the current timestamp.
     * The caller of this is responsible for not using any of the fresh timestamps previously served to it,
     * and must call getFreshTimestamps() to ensure it is using timestamps after the fastforward point.
     *
     * @param currentTimestamp the largest timestamp issued until the fast-forward call
     */
    @POST
    @Path("fast-forward")
    @Produces(MediaType.APPLICATION_JSON)
    void fastForwardTimestamp(@QueryParam("currentTimestamp") long currentTimestamp);
}
