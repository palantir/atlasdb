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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.sweep.CellWithTimestamp;
import java.util.List;
import java.util.concurrent.ExecutorService;

public interface CqlExecutor {
    /**
     * Returns a list of {@link CellWithTimestamp}s within the given {@code row}, starting at the (column, timestamp)
     * pair represented by ({@code startColumnInclusive}, {@code startTimestampExclusive}).
     */
    List<CellWithTimestamp> getTimestampsWithinRow(
            TableReference tableRef, byte[] row, byte[] startColumnInclusive, long startTimestampExclusive, int limit);

    /**
     * Returns a list of {@link CellWithTimestamp}s from cells within the given {@code rows}, starting at the given
     * {@code startRowInclusive}, potentially spanning across multiple rows. Will only return {@code limit} values,
     * so may not return cells from all of the rows provided.
     * @param executor is used for parallelizing the queries to Cassandra. Each row is fetched in a separate thread.
     * @param executorThreads the number of threads to use when fetching rows from Cassandra.
     */
    List<CellWithTimestamp> getTimestamps(
            TableReference tableRef, List<byte[]> rows, int limit, ExecutorService executor, Integer executorThreads);
}
