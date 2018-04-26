/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.queue;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.table.api.ColumnValue;
import com.palantir.common.persist.Persistable;

public final class SweepQueueUtils {
    public static final long TS_COARSE_GRANULARITY = 10_000_000L;
    public static final long TS_FINE_GRANULARITY = 50_000L;
    public static final long CAS_TS = 1L;

    private SweepQueueUtils() {
        // utility
    }

    public static long tsPartitionCoarse(long timestamp) {
        return timestamp / TS_COARSE_GRANULARITY;
    }

    public static long tsPartitionFine(long timestamp) {
        return timestamp / TS_FINE_GRANULARITY;
    }

    public static long partitionFineToCoarse(long partitionFine) {
        return partitionFine / (TS_COARSE_GRANULARITY / TS_FINE_GRANULARITY);
    }

    public static Cell toCell(Persistable row, ColumnValue<?> col) {
        return Cell.create(row.persistToBytes(), col.persistColumnName());
    }
}
