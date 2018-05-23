/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.todo;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.palantir.atlasdb.keyvalue.api.SweepResults;

@Path("/todos")
public interface TodoResource {
    @POST
    @Path("/")
    @Consumes(MediaType.APPLICATION_JSON)
    void addTodo(Todo todo);

    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    List<Todo> getTodoList();

    @GET
    @Path("/healthcheck")
    void isHealthy();

    @POST
    @Path("/snapshot")
    @Consumes(MediaType.APPLICATION_JSON)
    void storeSnapshot(String snapshot);

    @POST
    @Path("/sweep-snapshot-indices")
    @Produces(MediaType.APPLICATION_JSON)
    SweepResults sweepSnapshotIndices();

    @POST
    @Path("/sweep-snapshot-values")
    @Produces(MediaType.APPLICATION_JSON)
    SweepResults sweepSnapshotValues();
}
