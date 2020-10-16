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
package com.palantir.atlasdb.table.api;

import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import java.util.List;

/*
 * All named atlasdb tables should implement this interface.
 */
public interface AtlasDbNamedImmutableTable<ROW, COLUMN_VALUE, ROW_RESULT>
        extends AtlasDbImmutableTable<ROW, COLUMN_VALUE, ROW_RESULT> {
    List<ROW_RESULT> getRows(Iterable<ROW> rows);

    List<ROW_RESULT> getRows(Iterable<ROW> rows, ColumnSelection columnSelection);
}
