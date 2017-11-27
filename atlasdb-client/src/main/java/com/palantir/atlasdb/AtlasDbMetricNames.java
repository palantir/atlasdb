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

package com.palantir.atlasdb;

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

    public static final String SNAPSHOT_TRANSACTION_CELLS_READ = "numCellsRead";
    public static final String SNAPSHOT_TRANSACTION_CELLS_RETURNED = "numCellsReturnedAfterFiltering";
    public static final String SNAPSHOT_TRANSACTION_TOO_MANY_BYTES_READ = "tooManyBytesRead";
    public static final String SNAPSHOT_TRANSACTION_BYTES_WRITTEN = "bytesWritten";
}
