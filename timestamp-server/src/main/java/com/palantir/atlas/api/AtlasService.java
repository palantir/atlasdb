package com.palantir.atlas.api;

import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.annotation.Idempotent;

@Path("atlasdb")
public interface AtlasService {

    @Idempotent
    @Path("tables")
    @GET
    Set<String> getAllTableNames();

    @Idempotent
    @Path("metadata/{tableName:.+}")
    @GET
    TableMetadata getTableMetadata(@PathParam("tableName") String tableName);

    @Idempotent
    @Path("transaction")
    @POST
    TransactionToken startTransaction();

    @Idempotent
//    @Path("rows")
    @Path("rows{token : /(\\d+)?}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @POST
    TableRowResult getRows(@PathParam("token") @DefaultValue("-1") TransactionToken token,
                           TableRowSelection rows);

    @Idempotent
    @Path("cells/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @POST
    TableCellVal getCells(@PathParam("token") TransactionToken token,
                          TableCell cells);

    @Idempotent
    @Path("range/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @POST
    RangeToken getRange(@PathParam("token") TransactionToken token,
                        TableRange rangeRequest);

    @Idempotent
    @Path("put/{token}")
    @Consumes(MediaType.APPLICATION_JSON)
    @POST
    void put(@PathParam("token") TransactionToken token,
             TableCellVal data);

    @Idempotent
    @Path("delete/{token}")
    @Consumes(MediaType.APPLICATION_JSON)
    @POST
    void delete(@PathParam("token") TransactionToken token,
                TableCell cells);

    @Idempotent
    @Path("commit/{token}")
    @POST
    void commit(@PathParam("token") TransactionToken token);

    @Idempotent
    @Path("abort/{token}")
    @POST
    void abort(@PathParam("token") TransactionToken token);
}
