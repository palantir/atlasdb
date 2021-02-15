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
package com.palantir.atlasdb.api;

import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.annotation.Idempotent;
import java.util.Set;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/atlasdb")
public interface AtlasDbService {

    @Idempotent
    @GET
    @Path("tables")
    @Produces(MediaType.APPLICATION_JSON)
    Set<String> getAllTableNames();

    @Idempotent
    @GET
    @Path("metadata/{tableName}")
    @Produces(MediaType.APPLICATION_JSON)
    TableMetadata getTableMetadata(@PathParam("tableName") String tableName);

    @Idempotent
    @POST
    @Path("create-table/{tableName}")
    void createTable(@PathParam("tableName") String tableName);

    @Idempotent
    @POST
    @Path("transaction")
    @Produces(MediaType.APPLICATION_JSON)
    TransactionToken startTransaction();

    @Idempotent
    @POST
    @Path("rows/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    TableRowResult getRows(@PathParam("token") TransactionToken token, TableRowSelection rows);

    @Idempotent
    @POST
    @Path("cells/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    TableCellVal getCells(@PathParam("token") TransactionToken token, TableCell cells);

    @Idempotent
    @POST
    @Path("range/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    RangeToken getRange(@PathParam("token") TransactionToken token, TableRange rangeRequest);

    @Idempotent
    @POST
    @Path("put/{token}")
    @Consumes(MediaType.APPLICATION_JSON)
    void put(@PathParam("token") TransactionToken token, TableCellVal data);

    @Idempotent
    @POST
    @Path("delete/{token}")
    @Consumes(MediaType.APPLICATION_JSON)
    void delete(@PathParam("token") TransactionToken token, TableCell cells);

    @Idempotent
    @POST
    @Path("truncate-table/{tableName}")
    void truncateTable(@PathParam("tableName") String tableName);

    @Idempotent
    @POST
    @Path("commit/{token}")
    void commit(@PathParam("token") TransactionToken token);

    @Idempotent
    @POST
    @Path("abort/{token}")
    void abort(@PathParam("token") TransactionToken token);
}
