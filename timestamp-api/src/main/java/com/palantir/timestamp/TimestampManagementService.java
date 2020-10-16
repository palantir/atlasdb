/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.timestamp;

import com.palantir.logsafe.Safe;
import javax.annotation.CheckReturnValue;
import javax.annotation.meta.When;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path("/timestamp-management")
public interface TimestampManagementService {
    long SENTINEL_TIMESTAMP = Long.MIN_VALUE;
    String SENTINEL_TIMESTAMP_STRING =
            SENTINEL_TIMESTAMP + ""; // can't use valueOf/toString because we need a compile time constant!
    String PING_RESPONSE = "pong";

    /**
     * Updates the timestamp service to the currentTimestamp to ensure that all fresh timestamps issued after
     * this request are greater than the current timestamp.
     * The caller of this is responsible for not using any of the fresh timestamps previously served to it,
     * and must call getFreshTimestamps() to ensure it is using timestamps after the fastforward point.
     *
     * If currentTimestamp is unspecified (e.g. in a remote request), we will fast forward to the SENTINEL_TIMESTAMP.
     * This is intended to be Long.MIN_VALUE, so that regardless of the current state of the timestamp service
     * it is effectively a no-op. To improve usability, this method is also allowed to throw an exception in this case.
     *
     * @param currentTimestamp the largest timestamp issued until the fast-forward call
     */
    @POST
    @Path("fast-forward")
    @Produces(MediaType.APPLICATION_JSON)
    void fastForwardTimestamp(
            @Safe @QueryParam("currentTimestamp") @DefaultValue(SENTINEL_TIMESTAMP_STRING) long currentTimestamp);

    @GET
    @Path("ping")
    @Produces(MediaType.TEXT_PLAIN)
    @CheckReturnValue(when = When.NEVER)
    String ping();
}
