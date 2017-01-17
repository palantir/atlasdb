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
import com.palantir.atlasdb.keyvalue.impl.TableMappingNotFoundException;

public abstract class TableNameGetter {
    private final String tablePrefix;
    private final String overflowTablePrefix;
    private final TableNameMapper tableNameMapper;
    private final OracleTableNameUnmapper oracleTableNameUnmapper;
    private final boolean useTableMapping;

    public TableNameGetter(
            String tablePrefix,
            String overflowTablePrefix,
            boolean useTableMapping,
            TableNameMapper tableNameMapper) {
        this.tablePrefix = tablePrefix;
        this.overflowTablePrefix = overflowTablePrefix;
        this.useTableMapping = useTableMapping;
        this.tableNameMapper = tableNameMapper;
        this.oracleTableNameUnmapper = new OracleTableNameUnmapper();
    }

    public String generateShortTableName(ConnectionSupplier connectionSupplier, TableReference tableRef) {
        if (useTableMapping) {
            return tableNameMapper.getShortPrefixedTableName(connectionSupplier, tablePrefix, tableRef);
        }
        return getPrefixedTableName(tableRef);
    }

    public String generateShortOverflowTableName(ConnectionSupplier connectionSupplier, TableReference tableRef) {
        if (useTableMapping) {
            return tableNameMapper.getShortPrefixedTableName(connectionSupplier, overflowTablePrefix, tableRef);
        }
        return getPrefixedOverflowTableName(tableRef);
    }

    public String getInternalShortTableName(ConnectionSupplier connectionSupplier, TableReference tableRef)
            throws TableMappingNotFoundException {
        if (useTableMapping) {
            return oracleTableNameUnmapper.getShortTableNameFromMappingTable(connectionSupplier, tablePrefix, tableRef);
        }
        return getPrefixedTableName(tableRef);
    }

    public String getInternalShortOverflowTableName(ConnectionSupplier connectionSupplier, TableReference tableRef)
            throws TableMappingNotFoundException {
        if (useTableMapping) {
            return oracleTableNameUnmapper
                    .getShortTableNameFromMappingTable(connectionSupplier, overflowTablePrefix, tableRef);
        }
        return getPrefixedOverflowTableName(tableRef);
    }

    public String getPrefixedTableName(TableReference tableRef) {
        return tablePrefix + DbKvs.internalTableName(tableRef);
    }

    public String getPrefixedOverflowTableName(TableReference tableRef) {
        return overflowTablePrefix + DbKvs.internalTableName(tableRef);
    }

    public void clearCacheForTable(String fullTableName) {
        oracleTableNameUnmapper.clearCacheForTable(fullTableName);
    }
}
