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

package com.palantir.atlasdb.timelock.hackweek;

import java.util.List;

import com.palantir.atlasdb.protos.generated.TransactionService.CheckReadConflictsResponse;
import com.palantir.atlasdb.protos.generated.TransactionService.CommitWritesResponse;
import com.palantir.atlasdb.protos.generated.TransactionService.ImmutableTimestamp;
import com.palantir.atlasdb.protos.generated.TransactionService.TableCell;
import com.palantir.atlasdb.protos.generated.TransactionService.TableRange;
import com.palantir.atlasdb.protos.generated.TransactionService.Timestamp;
import com.palantir.atlasdb.protos.generated.TransactionService.TimestampRange;

public interface JamesTransactionService {
    ImmutableTimestamp getImmutableTimestamp();
    Timestamp getFreshTimestamp();
    TimestampRange startTransactions(long numberOfTransactions);
    CommitWritesResponse commitWrites(long startTimestamp, List<TableCell> writes);
    CheckReadConflictsResponse checkReadConflicts(long startTimestamp, List<TableCell> reads, List<TableRange> ranges);
    void unlock(List<Long> startTimestamps);
}
