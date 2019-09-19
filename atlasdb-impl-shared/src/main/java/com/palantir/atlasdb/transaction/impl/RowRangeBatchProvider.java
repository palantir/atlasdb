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
package com.palantir.atlasdb.transaction.impl;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;
import com.palantir.logsafe.Preconditions;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

public class RowRangeBatchProvider implements BatchProvider<RowResult<Value>> {
    private final KeyValueService keyValueService;
    private final TableReference tableRef;
    private final RangeRequest range;
    private final long timestamp;

    public RowRangeBatchProvider(
            KeyValueService keyValueService,
            TableReference tableRef,
            RangeRequest range,
            long timestamp) {
        this.keyValueService = keyValueService;
        this.tableRef = tableRef;
        this.range = range;
        this.timestamp = timestamp;
    }

    @Override
    public ClosableIterator<RowResult<Value>> getBatch(int batchSize, @Nullable byte[] lastToken) {
        RangeRequest.Builder newRange = range.getBuilder();
        if (lastToken != null) {
            newRange.startRowInclusive(RangeRequests.getNextStartRow(range.isReverse(), lastToken));
        }
        newRange.batchHint(batchSize);
        return keyValueService.getRange(tableRef, newRange.build(), timestamp);
    }

    @Override
    public boolean hasNext(byte[] lastToken) {
        if (RangeRequests.isTerminalRow(range.isReverse(), lastToken)) {
            return false;
        } else {
            byte[] nextStartRow = RangeRequests.getNextStartRow(range.isReverse(), lastToken);
            return !Arrays.equals(nextStartRow, range.getEndExclusive());
        }
    }

    @Override
    public byte[] getLastToken(List<RowResult<Value>> batch) {
        Preconditions.checkArgument(!batch.isEmpty());
        return batch.get(batch.size() - 1).getRowName();
    }
}
