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
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public final class OracleTableNameGetterImpl implements OracleTableNameGetter {
    private final String tablePrefix;
    private final String overflowTablePrefix;
    private final OracleTableNameMapper oracleTableNameMapper;
    private final OracleTableNameUnmapper oracleTableNameUnmapper;
    private final boolean useTableMapping;

    private OracleTableNameGetterImpl(
            OracleDdlConfig config, OracleTableNameMapper tableNameMapper, OracleTableNameUnmapper tableNameUnmapper) {
        this.tablePrefix = config.tablePrefix();
        this.overflowTablePrefix = config.overflowTablePrefix();
        this.useTableMapping = config.useTableMapping();

        this.oracleTableNameMapper = tableNameMapper;
        this.oracleTableNameUnmapper = tableNameUnmapper;
    }

    public static OracleTableNameGetter createDefault(OracleDdlConfig config) {
        return new OracleTableNameGetterImpl(config, new OracleTableNameMapper(), new OracleTableNameUnmapper());
    }

    public static OracleTableNameGetter createForTests(
            OracleDdlConfig config, OracleTableNameMapper tableNameMapper, OracleTableNameUnmapper tableNameUnmapper) {
        return new OracleTableNameGetterImpl(config, tableNameMapper, tableNameUnmapper);
    }

    @Override
    public String generateShortTableName(ConnectionSupplier connectionSupplier, TableReference tableRef) {
        if (useTableMapping) {
            return oracleTableNameMapper.getShortPrefixedTableName(connectionSupplier, tablePrefix, tableRef);
        }
        return getPrefixedTableName(tableRef);
    }

    @Override
    public String generateShortOverflowTableName(ConnectionSupplier connectionSupplier, TableReference tableRef) {
        if (useTableMapping) {
            return oracleTableNameMapper.getShortPrefixedTableName(connectionSupplier, overflowTablePrefix, tableRef);
        }
        return getPrefixedOverflowTableName(tableRef);
    }

    @Override
    public String getInternalShortTableName(ConnectionSupplier connectionSupplier, TableReference tableRef)
            throws TableMappingNotFoundException {
        if (useTableMapping) {
            return oracleTableNameUnmapper.getShortTableNameFromMappingTable(connectionSupplier, tablePrefix, tableRef);
        }
        return getPrefixedTableName(tableRef);
    }

    @Override
    public String getInternalShortOverflowTableName(ConnectionSupplier connectionSupplier, TableReference tableRef)
            throws TableMappingNotFoundException {
        if (useTableMapping) {
            return oracleTableNameUnmapper.getShortTableNameFromMappingTable(
                    connectionSupplier, overflowTablePrefix, tableRef);
        }
        return getPrefixedOverflowTableName(tableRef);
    }

    @Override
    public Set<TableReference> getTableReferencesFromShortTableNames(
            ConnectionSupplier connectionSupplier, Set<String> shortTableNames) {
        return getTableReferencesFromShortTableNamesWithPrefix(connectionSupplier, shortTableNames, tablePrefix);
    }

    @Override
    public Set<TableReference> getTableReferencesFromShortOverflowTableNames(
            ConnectionSupplier connectionSupplier, Set<String> shortTableNames) {
        return getTableReferencesFromShortTableNamesWithPrefix(
                connectionSupplier, shortTableNames, overflowTablePrefix);
    }

    private Set<TableReference> getTableReferencesFromShortTableNamesWithPrefix(
            ConnectionSupplier connectionSupplier, Set<String> shortTableNames, String tablePrefixToStrip) {
        Set<String> longTableNames = getLongTableNamesFromTableNames(connectionSupplier, shortTableNames);
        return longTableNames.stream()
                .peek(tableName -> {
                    if (!StringUtils.startsWithIgnoreCase(tableName, tablePrefixToStrip)) {
                        throw new SafeIllegalArgumentException(
                                "Long table name does not begin with prefix",
                                UnsafeArg.of("tableName", tableName),
                                SafeArg.of("prefix", tablePrefixToStrip));
                    }
                })
                .map(tableName -> StringUtils.removeStartIgnoreCase(tableName, tablePrefixToStrip))
                .map(TableReference::fromInternalTableName)
                .collect(Collectors.toSet());
    }

    private Set<String> getLongTableNamesFromTableNames(
            ConnectionSupplier connectionSupplier, Set<String> shortTableNames) {
        if (useTableMapping) {
            return new HashSet<>(oracleTableNameUnmapper
                    .getShortToLongTableNamesFromMappingTable(connectionSupplier, shortTableNames)
                    .values());
        }

        Set<String> lowerCasedMappedTables =
                oracleTableNameUnmapper
                        .getShortToLongTableNamesFromMappingTable(connectionSupplier, shortTableNames)
                        .keySet()
                        .stream()
                        .map(tableName -> tableName.toLowerCase(Locale.ROOT))
                        .collect(Collectors.toSet());
        // irrespective of the case-insensitive match, it is impossible to recover the original case-sensitive long
        // table name from the oracle table name, as Oracle uppercases table names
        return shortTableNames.stream()
                .map(tableName -> tableName.toLowerCase(Locale.ROOT))
                .filter(tableName -> !lowerCasedMappedTables.contains(tableName))
                .collect(Collectors.toSet());
    }

    @Override
    public String getPrefixedTableName(TableReference tableRef) {
        return tablePrefix + DbKvs.internalTableName(tableRef);
    }

    @Override
    public String getPrefixedOverflowTableName(TableReference tableRef) {
        return overflowTablePrefix + DbKvs.internalTableName(tableRef);
    }

    @Override
    public void clearCacheForTable(String fullTableName) {
        oracleTableNameUnmapper.clearCacheForTable(fullTableName);
    }
}
