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
package com.palantir.atlasdb.timelock.benchmarks;

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
public interface BenchmarksService {

    @GET
    @Path("/write-txn-rows")
    Map<String, Object> transactionWriteRows(
            @QueryParam("numClients") int numClients,
            @QueryParam("numRequestsPerClient") int numRequestsPerClient,
            @QueryParam("numRows") int numRows,
            @QueryParam("dataSize") int dataSize);

    @GET
    @Path("/write-txn-dynamic-columns")
    Map<String, Object> transactionWriteDynamicColumns(
            @QueryParam("numClients") int numClients,
            @QueryParam("numRequestsPerClient") int numRequestsPerClient,
            @QueryParam("numRows") int numRows,
            @QueryParam("dataSize") int dataSize);

    @GET
    @Path("/contended-write-txn")
    Map<String, Object> transactionWriteContended(
            @QueryParam("numClients") int numClients,
            @QueryParam("numRequestsPerClient") int numRequestsPerClient);


    @GET
    @Path("/lock-unlock-uncontended")
    Map<String, Object> lockAndUnlockUncontended(
            @QueryParam("numClients") int numClients,
            @QueryParam("numRequestsPerClient") int numRequestsPerClient);

    @GET
    @Path("/lock-unlock-contended")
    Map<String, Object> lockAndUnlockContended(
            @QueryParam("numClients") int numClients,
            @QueryParam("numRequestsPerClient") int numRequestsPerClient,
            @QueryParam("numDistinctLocks") int numDistinctLocks);

    @GET
    @Path("/read-txn-rows")
    Map<String, Object> transactionReadRows(
            @QueryParam("numClients") int numClients,
            @QueryParam("numRequestsPerClient") int numRequestsPerClient,
            @QueryParam("numRows") int numRows,
            @QueryParam("dataSize") int dataSize);

    @GET
    @Path("/kvs-write")
    Map<String, Object> kvsWrite(
            @QueryParam("numClients") int numClients,
            @QueryParam("numRequestsPerClient") int numRequestsPerClient);

    @GET
    @Path("/kvs-cas")
    Map<String, Object> kvsPutUnlessExists(
            @QueryParam("numClients") int numClients,
            @QueryParam("numRequestsPerClient") int numRequestsPerClient);

    @GET
    @Path("/kvs-read")
    Map<String, Object> kvsRead(
            @QueryParam("numClients") int numClients,
            @QueryParam("numRequestsPerClient") int numRequestsPerClient);

    @GET
    @Path("/timestamp")
    Map<String, Object> timestamp(
            @QueryParam("numClients") int numClients,
            @QueryParam("numRequestsPerClient") int numRequestsPerClient);

    @GET
    @Path("/range-scan-rows")
    Map<String, Object> rangeScanRows(
            @QueryParam("numClients") int numClients,
            @QueryParam("numRequestsPerClient") int numRequestsPerClient,
            @QueryParam("dataSize") int dataSize,
            @QueryParam("numRows") int numRows);

    @GET
    @Path("/range-scan-dynamic-columns")
    Map<String, Object> rangeScanDynamicColumns(
            @QueryParam("numClients") int numClients,
            @QueryParam("numRequestsPerClient") int numRequestsPerClient,
            @QueryParam("dataSize") int dataSize,
            @QueryParam("numRows") int numRows);

}
