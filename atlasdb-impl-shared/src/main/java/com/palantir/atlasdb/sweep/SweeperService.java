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
package com.palantir.atlasdb.sweep;

import com.palantir.logsafe.Safe;
import java.util.Optional;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

/**
 * Provides endpoints for sweeping a specific table.
 */
@Path("/sweep")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface SweeperService {

    default SweepTableResponse sweepTableFully(String tableName) {
        return sweepTable(
                tableName, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    default SweepTableResponse sweepTableFrom(String tableName, String startRow) {
        return sweepTable(
                tableName,
                Optional.of(startRow),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    /**
     * Sweeps a particular table.
     *
     * @param tableName the table to sweep, in the format namespace.table_name (e.g. myapp.users)
     * @param startRow (Optional) the row to start from, encoded as a hex string (e.g. 12345abcde)
     * @param fullSweep (Optional; default true) whether to sweep the full table; if false just runs one batch
     * @param maxCellTsPairsToExamine (Optional) see {@link SweepBatchConfig#maxCellTsPairsToExamine()}
     * @param candidateBatchSize (Optional) see {@link SweepBatchConfig#candidateBatchSize()}
     * @param deleteBatchSize (Optional) see {@link SweepBatchConfig#deleteBatchSize()}
     */
    @POST
    @Path("sweep-table")
    SweepTableResponse sweepTable(
            @QueryParam("tablename") String tableName,
            @QueryParam("startRow") Optional<String> startRow,
            @Safe @QueryParam("fullSweep") Optional<Boolean> fullSweep,
            @Safe @QueryParam("maxCellTsPairsToExamine") Optional<Integer> maxCellTsPairsToExamine,
            @Safe @QueryParam("candidateBatchSize") Optional<Integer> candidateBatchSize,
            @Safe @QueryParam("deleteBatchSize") Optional<Integer> deleteBatchSize);

    /**
     * Sweeps a table, but intentionally probabilistically skips a fraction of eligible entries to prevent issues that
     * might arise from having too many consecutive deletes in the underlying KVS. This is expected to be safe to
     * invoke at least twice without cleanup at the KVS level, but generally the expected usage is to run once, and
     * then invoke {@link #sweepTable(String, Optional, Optional, Optional, Optional, Optional)} to clean any
     * skipped values once KVS cleanup, such as compactions, is done.
     *
     * Implementation must respect the usual sweep guarantee: no (possibly in-flight) transaction may as a result of
     * this operation read a version (or lack of one) of a cell different than it would have read if this operation had
     * not run, and still succeed. In particular, this means that if any version of a cell is retained, then also the
     * latest version prior to the sweep timestamp of the same cell must not be removed (even if it is a delete).
     *
     * @param tableName the table to sweep, in the format namespace.table_name (e.g. myapp.users)
     * @param startRow (Optional) the row to start from, encoded as a hex string (e.g. 12345abcde)
     * @param fullSweep (Optional; default true) whether to sweep the full table; if false just runs one batch
     * @param maxCellTsPairsToExamine (Optional) see {@link SweepBatchConfig#maxCellTsPairsToExamine()}
     * @param candidateBatchSize (Optional) see {@link SweepBatchConfig#candidateBatchSize()}
     * @param deleteBatchSize (Optional) see {@link SweepBatchConfig#deleteBatchSize()}
     */
    @POST
    @Path("sweep-previously-conservative-now-thorough-table")
    SweepTableResponse sweepPreviouslyConservativeNowThoroughTable(
            @QueryParam("tablename") String tableName,
            @QueryParam("startRow") Optional<String> startRow,
            @Safe @QueryParam("fullSweep") Optional<Boolean> fullSweep,
            @Safe @QueryParam("maxCellTsPairsToExamine") Optional<Integer> maxCellTsPairsToExamine,
            @Safe @QueryParam("candidateBatchSize") Optional<Integer> candidateBatchSize,
            @Safe @QueryParam("deleteBatchSize") Optional<Integer> deleteBatchSize);
}
