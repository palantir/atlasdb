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
    private final boolean useTableMapping;

    public OracleTableNameGetter(OracleDdlConfig config, ConnectionSupplier conns, TableReference tableRef) {
        this.tablePrefix = config.tablePrefix();
        this.overflowTablePrefix = config.overflowTablePrefix();
        this.useTableMapping = config.useTableMapping();

        this.oracleTableNameMapper = new OracleTableNameMapper(conns);
        this.oracleTableNameUnmapper = new OracleTableNameUnmapper(conns);

        this.tableRef = tableRef;
    }

    public String generateShortTableName() {
        if (useTableMapping) {
            return oracleTableNameMapper.getShortPrefixedTableName(tablePrefix, tableRef);
        }
        return getPrefixedTableName();
    }

    public String generateShortOverflowTableName() {
        if (useTableMapping) {
            return oracleTableNameMapper.getShortPrefixedTableName(overflowTablePrefix, tableRef);
        }
        return getPrefixedOverflowTableName();
    }

    public String getInternalShortTableName() throws TableMappingNotFoundException {
        if (useTableMapping) {
            return oracleTableNameUnmapper.getShortTableNameFromMappingTable(tablePrefix, tableRef);
        }
        return getPrefixedTableName();
    }

    public String getInternalShortOverflowTableName() throws TableMappingNotFoundException {
        if (useTableMapping) {
            return oracleTableNameUnmapper.getShortTableNameFromMappingTable(overflowTablePrefix, tableRef);
        }
        return getPrefixedOverflowTableName();
    }

    public String getPrefixedTableName() {
        return tablePrefix + DbKvs.internalTableName(tableRef);
    }

    public String getPrefixedOverflowTableName() {
        return overflowTablePrefix + DbKvs.internalTableName(tableRef);
    }
}
