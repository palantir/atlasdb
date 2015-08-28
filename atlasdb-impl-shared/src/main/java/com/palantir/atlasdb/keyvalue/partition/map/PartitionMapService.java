package com.palantir.atlasdb.keyvalue.partition.map;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;
import com.palantir.atlasdb.keyvalue.partition.util.VersionedObject;

/**
 * Stores DynamicPartitionMap and allows to download it and to push an updated version.
 * This is meant to run on every endpoint next to the ednpoint KeyValueService.
 * 
 * Whenever the endpoint KeyValueService throws <code>VersionTooOldException</code>, the
 * corresponding PartitionMapService should be used by the caller to update its local
 * DynamicPartitionMap instance.
 * 
 * @author htarasiuk
 *
 */
@Path("/partition-map")
public interface PartitionMapService {

    @POST
    @Path("get")
    @Produces(MediaType.APPLICATION_JSON)
    DynamicPartitionMap get();

     @POST
     @Path("get-version")
     @Produces(MediaType.APPLICATION_JSON)
     long getVersion();

    @POST
    @Path("update-map")
    @Consumes(MediaType.APPLICATION_JSON)
    void update(DynamicPartitionMap partitionMap);

}
