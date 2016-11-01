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
package com.palantir.atlasdb.keyvalue.dbkvs;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;

public class OracleTableNameGetter {
    private final String tablePrefix;
    private final String overflowTablePrefix;
    private final TableReference tableRef;
    private final OracleTableNameMapper oracleTableNameMapper;
    private final OracleTableNameUnmapper oracleTableNameUnmapper;

    public OracleTableNameGetter(
            ConnectionSupplier conns,
            String tablePrefix,
            String overflowTablePrefix,
            TableReference tableRef) {
        this.tablePrefix = tablePrefix;
        this.overflowTablePrefix = overflowTablePrefix;
        this.tableRef = tableRef;
        this.oracleTableNameMapper = new OracleTableNameMapper(conns);
        this.oracleTableNameUnmapper = new OracleTableNameUnmapper(conns);
    }

    public String generateShortTableName() {
        return oracleTableNameMapper.getShortPrefixedTableName(tablePrefix, tableRef);
    }

    public String generateShortOverflowTableName() {
        return oracleTableNameMapper.getShortPrefixedTableName(overflowTablePrefix, tableRef);
    }

    public String getInternalShortTableName() throws TableMappingNotFoundException {
        return oracleTableNameUnmapper.getShortPrefixedTableName(tablePrefix, tableRef);
    }

    public String getInternalShortOverflowTableName() throws TableMappingNotFoundException {
        return oracleTableNameUnmapper.getShortPrefixedTableName(overflowTablePrefix, tableRef);
    }

    public String getPrefixedTableName() {
        return tablePrefix + DbKvs.internalTableName(tableRef);
    }

    public String getPrefixedOverflowTableName() {
        return overflowTablePrefix + DbKvs.internalTableName(tableRef);
    }
}
