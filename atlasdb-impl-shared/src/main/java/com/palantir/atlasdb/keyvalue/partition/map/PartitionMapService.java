/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.partition.map;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;

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
    @Path("get-map")
    @Produces(MediaType.APPLICATION_JSON)
    DynamicPartitionMap getMap();

     @POST
     @Path("get-map-version")
     @Produces(MediaType.APPLICATION_JSON)
     long getMapVersion();

    @POST
    @Path("update-map")
    @Consumes(MediaType.APPLICATION_JSON)
    @VisibleForTesting
    void updateMap(DynamicPartitionMap partitionMap);

    /**
     * This method must atomically check the version of partition map
     * and update it if the provided map is newer than the stored map.
     *
     * @param partitionMap
     * @return Version of the map that is stored in this service after
     * the operation. Must be greater or equal to the version of
     * <tt>partitionMap</tt>.
     */
    @POST
    @Path("update-map-if-newer")
    @Consumes(MediaType.APPLICATION_JSON)
    long updateMapIfNewer(DynamicPartitionMap partitionMap);

}
