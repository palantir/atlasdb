package com.palantir.atlas.api;

import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.annotation.Idempotent;

@Path("/atlasdb")
public interface AtlasService {

    @Idempotent
    @GET
    @Path("tables")
    @Produces(MediaType.APPLICATION_JSON)
    Set<String> getAllTableNames();

    @Idempotent
    @GET
    @Path("metadata/{tableName:.+}")
    @Produces(MediaType.APPLICATION_JSON)
    TableMetadata getTableMetadata(@PathParam("tableName") String tableName);

    @Idempotent
    @POST
    @Path("transaction")
    @Produces(MediaType.APPLICATION_JSON)
    long startTransaction();

    @Idempotent
    @POST
    @Path("rows/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    TableRowResult getRows(@PathParam("token") Long token,
                           TableRowSelection rows);

    @Idempotent
    @POST
    @Path("cells/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    TableCellVal getCells(@PathParam("token") Long token,
                          TableCell cells);

    @Idempotent
    @POST
    @Path("range/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    RangeToken getRange(@PathParam("token") Long token,
                        TableRange rangeRequest);

    @Idempotent
    @POST
    @Path("put/{token}")
    @Consumes(MediaType.APPLICATION_JSON)
    void put(@PathParam("token") Long token,
             TableCellVal data);

    @Idempotent
    @POST
    @Path("delete/{token}")
    @Consumes(MediaType.APPLICATION_JSON)
    void delete(@PathParam("token") Long token,
                TableCell cells);

    @Idempotent
    @POST
    @Path("commit/{token}")
    void commit(@PathParam("token") Long token);

    @Idempotent
    @POST
    @Path("abort/{token}")
    void abort(@PathParam("token") Long token);
}
