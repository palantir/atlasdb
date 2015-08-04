/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.table.api;

import java.util.List;
import java.util.concurrent.ExecutorService;

import com.google.common.base.Optional;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;

/*
 * All named atlasdb tables should implement this interface.
 */
public interface AtlasDbNamedImmutableTable<ROW, COLUMN_VALUE, ROW_RESULT> extends
            AtlasDbImmutableTable<ROW, COLUMN_VALUE, ROW_RESULT> {
    public Optional<ROW_RESULT> getRow(ROW row);
    public Optional<ROW_RESULT> getRow(ROW row, ColumnSelection columnSelection);
    public List<ROW_RESULT> getRows(Iterable<ROW> rows);
    public List<ROW_RESULT> getRows(Iterable<ROW> rows, ColumnSelection columnSelection);
    public List<ROW_RESULT> getAsyncRows(Iterable<ROW> rows, ExecutorService exec);
    public List<ROW_RESULT> getAsyncRows(Iterable<ROW> rows, ColumnSelection columnSelection, ExecutorService exec);
}
