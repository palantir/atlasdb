/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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

public final class ColumnFetchMode {
    private final int columnsToFetch;

    private ColumnFetchMode(int columnsToFetch) {
        this.columnsToFetch = columnsToFetch;
    }

    public static ColumnFetchMode fetchAll() {
        return new ColumnFetchMode(Integer.MAX_VALUE);
    }

    public static ColumnFetchMode fetchAtMost(int numColumnsToFetch) {
        return new ColumnFetchMode(numColumnsToFetch);
    }

    public int getColumnsToFetch() {
        return columnsToFetch;
    }
}
