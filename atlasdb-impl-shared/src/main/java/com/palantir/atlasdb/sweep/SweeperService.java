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
package com.palantir.atlasdb.sweep;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
     * Sweep a particular table from EMPTY startRow with default {@link SweepBatchConfig}.
     */
    @POST
    @Path("sweep-table")
    @Produces(MediaType.APPLICATION_JSON)
    void sweepTable(@QueryParam("tablename") String tableName);

    /**
     * Sweep a particular table from specified startRow with default {@link SweepBatchConfig}.
     */
    @POST
    @Path("sweep-table-from-row")
    @Produces(MediaType.APPLICATION_JSON)
    void sweepTableFromStartRow(
            @QueryParam("tablename") String tableName,
            @Nonnull @QueryParam("startRow") String startRow);

    /**
     * Sweep a particular table from specified startRow with specified {@link SweepBatchConfig} parameters.
     */
    @POST
    @Path("sweep-table-from-row-with-batch")
    @Produces(MediaType.APPLICATION_JSON)
    void sweepTableFromStartRowWithBatchConfig(
            @QueryParam("tablename") String tableName,
            @Nullable @QueryParam("startRow") String startRow,
            @Nullable @QueryParam("maxCellTsPairsToExamine") Integer maxCellTsPairsToExamine,
            @Nullable @QueryParam("candidateBatchSize") Integer candidateBatchSize,
            @Nullable @QueryParam("deleteBatchSize") Integer deleteBatchSize);
}
