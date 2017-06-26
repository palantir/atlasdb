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
package com.palantir.atlasdb.sweeperservice;

import javax.annotation.Nonnull;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

/**
 * Provides endpoints for sweeping a specific table.
 */
@Path("/sweep")
public interface SweeperService {
    /**
     * Attempt to sweep a particular table.
     * @return a boolean which is true on success and false otherwise
     */
    @POST
    @Path("sweep-table")
    @Produces(MediaType.APPLICATION_JSON)
    void sweepTable(@QueryParam("tablename") String tableName);

    @POST
    @Path("sweep-table-from-row")
    @Produces(MediaType.APPLICATION_JSON)
    void sweepTableFromStartRow(@QueryParam("tablename") String tableName,
            @Nonnull @QueryParam("startRow") String startRow);

    @POST
    @Path("sweep-table-from-row-with-batch")
    @Produces(MediaType.APPLICATION_JSON)
    void sweepTableFromStartRowWithBatchConfig(@QueryParam("tablename") String tableName,
            @Nonnull @QueryParam("startRow") String startRow,
            @DefaultValue("100") @QueryParam("maxCellTsPairsToExamine") int maxCellTsPairsToExamine,
            @DefaultValue("100") @QueryParam("candidateBatchSize") int candidateBatchSize,
            @DefaultValue("1000") @QueryParam("deleteBatchSize") int deleteBatchSize);
}
