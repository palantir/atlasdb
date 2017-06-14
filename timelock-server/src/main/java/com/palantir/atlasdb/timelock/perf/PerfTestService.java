/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.perf;

import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path("/perf")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface PerfTestService {

    @GET
    @Path("/write-txn")
    Map<String, Object> writeTransactionPerf(
            @QueryParam("numClients") int numClients,
            @QueryParam("numRequestsPerClient") int numRequestsPerClient);

    @GET
    @Path("/read-txn")
    Map<String, Object> readTransactionPerf(
            @QueryParam("numClients") int numClients,
            @QueryParam("numRequestsPerClient") int numRequestsPerClient);

    @GET
    @Path("/kvs-write")
    Map<String, Object> kvsWritePerf(
            @QueryParam("numClients") int numClients,
            @QueryParam("numRequestsPerClient") int numRequestsPerClient);

    @GET
    @Path("/kvs-cas")
    Map<String, Object> kvsCasPerf(
            @QueryParam("numClients") int numClients,
            @QueryParam("numRequestsPerClient") int numRequestsPerClient);

    @GET
    @Path("/kvs-read")
    Map<String, Object> kvsReadPerf(
            @QueryParam("numClients") int numClients,
            @QueryParam("numRequestsPerClient") int numRequestsPerClient);

    @GET
    @Path("/timestamp")
    Map<String, Object> timestampPerf(
            @QueryParam("numClients") int numClients,
            @QueryParam("numRequestsPerClient") int numRequestsPerClient);

}