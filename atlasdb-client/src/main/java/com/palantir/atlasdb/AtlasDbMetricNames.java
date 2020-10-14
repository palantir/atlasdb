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
package com.palantir.atlasdb;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

public final class AtlasDbMetricNames {

    private AtlasDbMetricNames() {
        // Utility
    }

    public final class CellFilterMetrics {
        private CellFilterMetrics() {
            // Utility
        }

        public static final String NOT_LATEST_VISIBLE_VALUE = "notLatestVisibleValueCellFilterCount";
        public static final String COMMIT_TS_GREATER_THAN_TRANSACTION_TS = "commitTsGreaterThatTxTsCellFilterCount";
        public static final String INVALID_START_TS = "invalidStartTsTsCellFilterCount";
        public static final String INVALID_COMMIT_TS = "invalidCommitTsCellFilterCount";
        public static final String EMPTY_VALUE = "emptyValuesCellFilterCount";
    }

    public static final String LIBRARY_ORIGIN_TAG = "libraryOrigin";
    public static final String LIBRARY_ORIGIN_VALUE = "atlasdb";

    public static final String SNAPSHOT_TRANSACTION_CELLS_READ = "numCellsRead";
    public static final String SNAPSHOT_TRANSACTION_CELLS_RETURNED = "numCellsReturnedAfterFiltering";
    public static final String SNAPSHOT_TRANSACTION_TOO_MANY_BYTES_READ = "tooManyBytesRead";
    public static final String SNAPSHOT_TRANSACTION_BYTES_WRITTEN = "bytesWritten";

    public static final String CELLS_EXAMINED = "cellTimestampPairsExamined";
    public static final String CELLS_SWEPT = "staleValuesDeleted";
    public static final String TIME_SPENT_SWEEPING = "sweepTimeSweeping";
    public static final String TIME_ELAPSED_SWEEPING = "sweepTimeElapsedSinceStart";
    public static final String SWEEP_ERROR = "sweepError";

    public static final String TIMELOCK_SUCCESSFUL_REQUEST = "timelockSuccessfulRequest";
    public static final String TIMELOCK_FAILED_REQUEST = "timelockFailedRequest";

    public static final String TAG_STRATEGY = "strategy";
    public static final String TAG_CONSERVATIVE = "conservative";
    public static final String TAG_THOROUGH = "thorough";
    public static final String ENQUEUED_WRITES = "enqueuedWrites";
    public static final String ENTRIES_READ = "entriesRead";
    public static final String TOMBSTONES_PUT = "tombstonesPut";
    public static final String ABORTED_WRITES_DELETED = "abortedWritesDeleted";
    public static final String SWEEP_TS = "sweepTimestamp";
    public static final String LAST_SWEPT_TS = "lastSweptTimestamp";
    public static final String LAG_MILLIS = "millisSinceLastSweptTs";
    public static final String BATCH_SIZE_MEAN = "batchSizeMean";
    public static final String SWEEP_DELAY = "sweepDelay";
    public static final Set<String> TARGETED_SWEEP_PROGRESS_METRIC_NAMES = ImmutableSet.of(
            ENQUEUED_WRITES,
            ENTRIES_READ,
            TOMBSTONES_PUT,
            ABORTED_WRITES_DELETED,
            SWEEP_TS,
            LAST_SWEPT_TS,
            LAG_MILLIS,
            BATCH_SIZE_MEAN,
            SWEEP_DELAY);

    public static final String SWEEP_OUTCOME = "outcome";
    public static final String TAG_OUTCOME = "status";

    public static final String ENQUEUED_CELLS = "enqueuedCells";
    public static final String DELETED_CELLS = "deletedCells";
    public static final String SCRUBBED_CELLS = "scrubbedCells";
    public static final String SCRUB_RETRIES = "retriedBatches";

    public static final String TAG_CURRENT_SUSPECTED_LEADER = "isCurrentSuspectedLeader";
    public static final String TAG_CLIENT = "client";
    public static final String TAG_PAXOS_USE_CASE = "paxosUseCase";
    public static final String TAG_REMOTE_HOST = "remoteHost";

    public static final String COORDINATION_LAST_VALID_BOUND = "lastValidBound";
    public static final String COORDINATION_CURRENT_TRANSACTIONS_SCHEMA_VERSION = "currentTransactionsSchemaVersion";
    public static final String COORDINATION_EVENTUAL_TRANSACTIONS_SCHEMA_VERSION = "eventualTransactionsSchemaVersion";

    public static final String LEGACY_READ = "legacyRead";
    public static final String LEGACY_WRITE = "legacyWrite";
}
