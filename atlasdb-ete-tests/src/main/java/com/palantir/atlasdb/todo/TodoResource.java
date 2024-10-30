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
package com.palantir.atlasdb.todo;

import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import java.util.List;

@Path("/todos")
public interface TodoResource {
    @POST
    @Path("/")
    @Consumes(MediaType.APPLICATION_JSON)
    void addTodo(Todo todo);

    @POST
    @Path("/addWithId")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    long addTodoWithIdAndReturnTimestamp(@QueryParam("id") long id, Todo todo);

    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    List<Todo> getTodoList();

    @GET
    @Path("/doesNotExistBeforeTimestamp")
    @Produces(MediaType.APPLICATION_JSON)
    boolean doesNotExistBeforeTimestamp(@QueryParam("id") long id, @QueryParam("timestamp") long timestamp);

    @GET
    @Path("/healthcheck")
    void isHealthy();

    @POST
    @Path("/snapshot")
    @Consumes(MediaType.APPLICATION_JSON)
    void storeSnapshot(String snapshot);

    @POST
    @Path("/unmarked-snapshot")
    @Consumes(MediaType.APPLICATION_JSON)
    void storeUnmarkedSnapshot(String snapshot);

    @POST
    @Path("/targeted-sweep")
    void runIterationOfTargetedSweep();

    @POST
    @Path("/sweep-snapshot-indices")
    @Produces(MediaType.APPLICATION_JSON)
    SweepResults sweepSnapshotIndices();

    @POST
    @Path("/sweep-snapshot-values")
    @Produces(MediaType.APPLICATION_JSON)
    SweepResults sweepSnapshotValues();

    @POST
    @Path("cells-deleted")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    long numberOfCellsDeleted(TableReference tableRef);

    @POST
    @Path("cells-deleted-and-swept")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    long numberOfCellsDeletedAndSwept(TableReference tableRef);

    @POST
    @Path("truncate-tables")
    @Consumes(MediaType.APPLICATION_JSON)
    void truncate();

    @POST
    @Path("/namespaced")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    long addNamespacedTodoWithIdAndReturnTimestamp(
            @QueryParam("id") long id, @QueryParam("namespace") String namespace, Todo todo);

    @GET
    @Path("/namespacedDoesNotExistBeforeTimestamp")
    @Produces(MediaType.APPLICATION_JSON)
    boolean namespacedTodoDoesNotExistBeforeTimestamp(
            @QueryParam("id") long id,
            @QueryParam("timestamp") long timestamp,
            @QueryParam("namespace") String namespace);
}
