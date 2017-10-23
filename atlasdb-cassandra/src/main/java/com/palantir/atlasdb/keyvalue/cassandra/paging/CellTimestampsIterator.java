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

package com.palantir.atlasdb.keyvalue.cassandra.paging;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.AbstractIterator;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CellTimestampsFetch;
import com.palantir.atlasdb.keyvalue.cassandra.CqlExecutor;

@NotThreadSafe
public class CellTimestampsIterator extends AbstractIterator<List<CellWithTimestamps>> {
    private final CqlExecutor cqlExecutor;
    private final TableReference tableRef;
    private final int batchSize;

    byte[] nextStartRow;

    public CellTimestampsIterator(
            CqlExecutor cqlExecutor,
            TableReference tableRef,
            byte[] startRowInclusive,
            int batchSize) {
        this.cqlExecutor = cqlExecutor;
        this.tableRef = tableRef;
        this.batchSize = batchSize;

        this.nextStartRow = startRowInclusive;
    }

    @Override
    protected List<CellWithTimestamps> computeNext() {
        List<CellWithTimestamps> next = new CellTimestampsFetch(
                cqlExecutor,
                tableRef,
                nextStartRow,
                batchSize).execute();

        if (next.isEmpty()) {
            return endOfData();
        }

        nextStartRow = RangeRequests.nextLexicographicName(
                next.get(next.size() - 1).cell().getRowName());

        return next;
    }

}
