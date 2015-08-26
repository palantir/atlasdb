package com.palantir.atlasdb.keyvalue.partition;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;
import com.palantir.atlasdb.keyvalue.partition.util.VersionedObject;

@Path("/partition-map")
public interface PartitionMapService {

    @POST
    @Path("get")
    @Produces(MediaType.APPLICATION_JSON)
    VersionedObject<PartitionMap> get();

    @POST
    @Path("get-version")
    @Produces(MediaType.APPLICATION_JSON)
    long getVersion();

    @POST
    @Path("update")
    @Consumes(MediaType.APPLICATION_JSON)
    void update(@QueryParam("version") long version, PartitionMap partitionMap);

}
