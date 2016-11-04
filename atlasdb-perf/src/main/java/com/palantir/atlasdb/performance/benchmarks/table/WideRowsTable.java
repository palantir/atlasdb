/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.performance.benchmarks.table;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import com.palantir.atlasdb.keyvalue.api.TableReference;

/**
 * State class for creating a single Atlas table with {@link #NUM_ROWS} wide rows, each with {@link #NUM_COLS_PER_ROW}
 * columns.
 */
@State(Scope.Benchmark)
public class WideRowsTable extends AbstractWideRowsTable {
    public static final int NUM_ROWS = 10000;
    public static final int NUM_COLS_PER_ROW = 20;

    public WideRowsTable() {
        super(NUM_ROWS, NUM_COLS_PER_ROW);
    }

    @Override
    public TableReference getTableRef() {
        return Tables.TABLE_REF;
    }
}
