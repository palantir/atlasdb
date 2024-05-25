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

package com.palantir.atlasdb.transaction.api.snapshot;

import com.palantir.atlasdb.keyvalue.api.TableReference;

public interface KeyValueSnapshotEventRecorder {
    void recordCellsRead(TableReference tableReference, long cellsRead);

    void recordCellsReturned(TableReference tableReference, long cellsReturned);

    void recordManyBytesReadForTable(TableReference tableReference, long bytesRead);

    void recordFilteredSweepSentinel(TableReference tableReference);

    void recordFilteredUncommittedTransaction(TableReference tableReference);

    void recordFilteredTransactionCommittingAfterOurStart(TableReference tableReference);

    void recordRolledBackOtherTransaction();

    void recordEmptyValueRead(TableReference tableReference);
}
