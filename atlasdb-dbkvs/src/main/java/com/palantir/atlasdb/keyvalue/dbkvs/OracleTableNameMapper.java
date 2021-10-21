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

import com.google.common.base.Preconditions;
import com.google.common.math.IntMath;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.nexus.db.sql.AgnosticResultSet;

public class OracleTableNameMapper {
    public static final int SUFFIX_NUMBER_LENGTH = 4;
    public static final int MAX_NAMESPACE_LENGTH = 2;
    private static final int ONE_UNDERSCORE = 1;
    private static final int NAMESPACED_TABLE_NAME_LENGTH =
            AtlasDbConstants.ATLASDB_ORACLE_TABLE_NAME_LIMIT - (ONE_UNDERSCORE + SUFFIX_NUMBER_LENGTH);

    public String getShortPrefixedTableName(
            ConnectionSupplier connectionSupplier, String tablePrefix, TableReference tableRef) {
        Preconditions.checkState(
                tablePrefix.length() <= AtlasDbConstants.MAX_TABLE_PREFIX_LENGTH,
                "The tablePrefix can be at most %s characters long",
                AtlasDbConstants.MAX_TABLE_PREFIX_LENGTH);

        TableReference shortenedNamespaceTableRef = truncateNamespace(tableRef);

        String prefixedTableName = tablePrefix + DbKvs.internalTableName(shortenedNamespaceTableRef);
        String truncatedTableName = truncate(prefixedTableName, NAMESPACED_TABLE_NAME_LENGTH);

        String fullTableName = tablePrefix + DbKvs.internalTableName(tableRef);
        return truncatedTableName + "_" + getTableNumber(connectionSupplier, fullTableName, truncatedTableName);
    }

    private TableReference truncateNamespace(TableReference tableRef) {
        if (tableRef.getNamespace().isEmptyNamespace()) {
            return tableRef;
        }
        String namespace = tableRef.getNamespace().getName();
        namespace = truncate(namespace, MAX_NAMESPACE_LENGTH);
        return TableReference.create(Namespace.create(namespace), tableRef.getTableName());
    }

    private String truncate(String name, int length) {
        return name.substring(0, Math.min(name.length(), length));
    }

    private String getTableNumber(
            ConnectionSupplier connectionSupplier, String fullTableName, String truncatedTableName) {
        int tableSuffixNumber = getNextTableNumber(connectionSupplier, truncatedTableName);
        long maxTablesWithSamePrefix = IntMath.checkedPow(10, SUFFIX_NUMBER_LENGTH);
        if (tableSuffixNumber >= maxTablesWithSamePrefix) {
            throw new IllegalArgumentException(String.format(
                    "Cannot create any more tables with name starting with %s. "
                            + "%d tables might have already been created. "
                            + "Please rename the table %s.",
                    truncatedTableName, maxTablesWithSamePrefix, fullTableName));
        }
        return String.format("%0" + SUFFIX_NUMBER_LENGTH + "d", tableSuffixNumber);
    }

    private int getNextTableNumber(ConnectionSupplier connectionSupplier, String truncatedTableName) {
        AgnosticResultSet results = connectionSupplier
                .get()
                .selectResultSetUnregisteredQuery(
                        "SELECT short_table_name "
                                + "FROM " + AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE
                                + " WHERE LOWER(short_table_name) LIKE LOWER(?||'\\_____%') ESCAPE '\\'"
                                + " ORDER BY short_table_name DESC",
                        truncatedTableName);
        return getTableNumberFromTableNames(truncatedTableName, results);
    }

    private int getTableNumberFromTableNames(String truncatedTableName, AgnosticResultSet results) {
        for (int i = 0; i < results.size(); i++) {
            String shortName = results.get(i).getString("short_table_name");
            try {
                return Integer.parseInt(shortName.substring(truncatedTableName.length() + 1)) + 1;
            } catch (NumberFormatException e) {
                // Table in different format - Do nothing;
            }
        }
        return 0;
    }
}
