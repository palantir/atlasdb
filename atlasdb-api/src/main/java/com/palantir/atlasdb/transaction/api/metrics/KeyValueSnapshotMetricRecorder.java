/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.api.metrics;

import com.palantir.atlasdb.keyvalue.api.TableReference;

/**
 * Updates metrics for work done as part of reading a transactional snapshot from the key-value-service.
 */
public interface KeyValueSnapshotMetricRecorder {
    /**
     * Track that for the given table, the given number of cells was read from the key-value-service (including
     * cells that could not be returned due to filtering).
     */
    void recordCellsRead(TableReference tableReference, long cellsRead);

    /**
     * Track that for the given table, the given number of cells was returned to the user (excluding cells that were
     * filtered out internally).
     */
    void recordCellsReturned(TableReference tableReference, long cellsReturned);

    /**
     * Track that a large number of bytes was read in a single call to an individual table, which could be indicative
     * of unexpected behaviour.
     */
    void recordManyBytesReadForTable(TableReference tableReference, long bytesRead);

    void recordFilteredSweepSentinel(TableReference tableReference);

    void recordFilteredUncommittedTransaction(TableReference tableReference);

    void recordFilteredTransactionCommittingAfterOurStart(TableReference tableReference);

    /**
     * Empty values are AtlasDB tombstones (only written on delete), and should not be returned to the user.
     */
    void recordFilteredEmptyValues(TableReference tableReference, long cellsRead);

    void recordRolledBackOtherTransaction();
}
