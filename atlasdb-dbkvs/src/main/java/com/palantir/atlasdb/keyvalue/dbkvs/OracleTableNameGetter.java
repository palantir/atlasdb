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
package com.palantir.atlasdb.keyvalue.dbkvs;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.common.exception.TableMappingNotFoundException;

public class OracleTableNameGetter {
    private final String tablePrefix;
    private final String overflowTablePrefix;
    private final OracleTableNameMapper oracleTableNameMapper;
    private final OracleTableNameUnmapper oracleTableNameUnmapper;
    private final boolean useTableMapping;

    public OracleTableNameGetter(OracleDdlConfig config) {
        this.tablePrefix = config.tablePrefix();
        this.overflowTablePrefix = config.overflowTablePrefix();
        this.useTableMapping = config.useTableMapping();

        this.oracleTableNameMapper = new OracleTableNameMapper();
        this.oracleTableNameUnmapper = new OracleTableNameUnmapper();
    }

    public String generateShortTableName(ConnectionSupplier connectionSupplier, TableReference tableRef) {
        if (useTableMapping) {
            return oracleTableNameMapper.getShortPrefixedTableName(connectionSupplier, tablePrefix, tableRef);
        }
        return getPrefixedTableName(tableRef);
    }

    public String generateShortOverflowTableName(ConnectionSupplier connectionSupplier, TableReference tableRef) {
        if (useTableMapping) {
            return oracleTableNameMapper.getShortPrefixedTableName(connectionSupplier, overflowTablePrefix, tableRef);
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
            return oracleTableNameUnmapper.getShortTableNameFromMappingTable(
                    connectionSupplier, overflowTablePrefix, tableRef);
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
