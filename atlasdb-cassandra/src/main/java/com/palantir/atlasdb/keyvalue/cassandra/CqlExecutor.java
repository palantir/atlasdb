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

package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.List;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.sweep.CellWithTimestamp;

public interface CqlExecutor {
    /**
     * Returns a list of {@link CellWithTimestamp}s within the given {@code row}, starting at the (column, timestamp)
     * pair represented by ({@code startColumnInclusive}, {@code startTimestampExclusive}).
     */
    List<CellWithTimestamp> getTimestampsWithinRow(
            TableReference tableRef,
            byte[] row,
            byte[] startColumnInclusive,
            long startTimestampExclusive,
            int limit);
    /**
     * Returns a list of {@link CellWithTimestamp}s within the given {@code row}, starting at the given
     * {@code startRowInclusive}, potentially spanning across multiple rows.
     */
    List<CellWithTimestamp> getTimestamps(
            TableReference tableRef,
            byte[] startRowInclusive,
            byte[] endRowInclusive,
            int limit);
}
