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

import com.google.common.collect.Multimap;

/*
 * All dynamic atlasdb tables should implement this interface.
 */
public interface AtlasDbDynamicMutableTable<ROW, COLUMN, COLUMN_VALUE, ROW_RESULT>
        extends AtlasDbDynamicImmutableTable<ROW, COLUMN, COLUMN_VALUE, ROW_RESULT> {
    void delete(Multimap<ROW, COLUMN> values);

    void delete(ROW row, COLUMN column);

    /**
     * This method needs to load the row so it knows what to delete.
     * <p>
     * Since transactions use snapshot isolation, this will only delete cells that
     * are readable.  If concurrent transactions put additional cells, they will not be
     * deleted.  If this is an issue for your application you must use additional
     * locking or have readers and writers touch the same cell to materialize the
     * conflict.
     */
    void delete(Iterable<ROW> rows);
}
