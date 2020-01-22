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
package com.palantir.nexus.db.sql;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.DBType;
import com.palantir.sql.ResultSets;
import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.Map;

public class AgnosticResultSetImpl implements AgnosticResultSet {
    private final List<List<Object>> results;
    private final DBType dbType;
    private final Map<String, Integer> columnMap;

    public AgnosticResultSetImpl(List<List<Object>> rs, DBType type, Map<String, Integer> columnMap) throws PalantirSqlException {
        results = rs;
        dbType = type;
        this.columnMap = columnMap;
    }

    public AgnosticResultSetImpl(List<List<Object>> rs, DBType type, ResultSetMetaData meta) throws PalantirSqlException {
        results = rs;
        dbType = type;

        /* Build a map containing both all lower and all upper case versions of
         * the column name. This requires double the memory for the map, but
         * avoids the CPU and GC cost of case conversion upon lookup compared
         * to using something like newTreeMap(String.CASE_INSENSITIVE_ORDER).
         *
         * This also requires users of this map to convert keys to either all
         * lower or all upper case for access.
         */
        int columnCount = ResultSets.getColumnCount(meta);
        columnMap = Maps.newHashMapWithExpectedSize(2 * columnCount);
        for (int i = 0; i < columnCount; i++) {
            columnMap.put(ResultSets.getColumnLabel(meta, i + 1).toLowerCase(), i);
            columnMap.put(ResultSets.getColumnLabel(meta, i + 1).toUpperCase(), i);
        }
    }

    @Override
    public List<AgnosticResultRow> rows() {
        List<AgnosticResultRow> allRows = Lists.newArrayListWithCapacity(size());
        for (List<Object> list : results) {
            allRows.add(new AgnosticResultRowImpl(list, dbType, columnMap));
        }
        return allRows;
    }

    @Override
    public AgnosticResultRow get(int row) {
        return new AgnosticResultRowImpl(results.get(row), dbType, columnMap);
    }

    @Override
    public int size() {
        return results == null ? 0 : results.size();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("AgnosticResultSetImpl {\n"); //$NON-NLS-1$
        boolean first = true;
        for (AgnosticResultRow row : rows()) {
            if (!first) {
                sb.append(",\n"); //$NON-NLS-1$
            }

            first = false;
            sb.append(row);
        }
        return sb.append("\n}").toString(); //$NON-NLS-1$
    }
}
