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

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.nexus.db.sql.AgnosticResultSet;

public abstract class TableNameMapper {
    public static final int SUFFIX_NUMBER_LENGTH = 6;
    private final int prefixedTableNameLength = getMaxTableNameLength() - SUFFIX_NUMBER_LENGTH;

    public abstract int getMaxTableNameLength();
    public abstract int getShortenedNamespaceLength();

    public String getShortPrefixedTableName(
            ConnectionSupplier connectionSupplier,
            String tablePrefix,
            TableReference tableRef) {
        Preconditions.checkState(tablePrefix.length() <= AtlasDbConstants.MAX_TABLE_PREFIX_LENGTH,
                "The tablePrefix can be at most %s characters long", AtlasDbConstants.MAX_TABLE_PREFIX_LENGTH);

        TableReference shortenedNamespaceTableRef = truncateNamespace(tableRef);

        String prefixedTableName = tablePrefix + DbKvs.internalTableName(shortenedNamespaceTableRef);
        String truncatedTableName = truncate(prefixedTableName, prefixedTableNameLength);

        String fullTableName = tablePrefix + DbKvs.internalTableName(tableRef);
        return truncatedTableName + getTableNumberSuffix(connectionSupplier, fullTableName, truncatedTableName);
    }

    private TableReference truncateNamespace(TableReference tableRef) {
        if (tableRef.getNamespace().isEmptyNamespace()) {
            return tableRef;
        }
        String namespace = tableRef.getNamespace().getName();
        namespace = truncate(namespace, getShortenedNamespaceLength());
        return TableReference.create(Namespace.create(namespace), tableRef.getTablename());
    }

    private String truncate(String name, int length) {
        return name.substring(0, Math.min(name.length(), length));
    }

    private String getTableNumberSuffix(
            ConnectionSupplier connectionSupplier,
            String fullTableName,
            String truncatedTableName) {
        int tableSuffixNumber = getNextTableNumber(connectionSupplier, truncatedTableName);
        if (tableSuffixNumber >= 100_000) {
            throw new IllegalArgumentException(
                    "Cannot create any more tables with name starting with " + truncatedTableName
                    + ". 100,000 tables might have already been created. Please rename the table " + fullTableName);
        }
        return "_" + String.format("%05d", tableSuffixNumber);
    }

    private int getNextTableNumber(ConnectionSupplier connectionSupplier, String truncatedTableName) {
        AgnosticResultSet results = connectionSupplier.get().selectResultSetUnregisteredQuery(
                "SELECT short_table_name "
                + "FROM " + AtlasDbConstants.DBKVS_NAME_MAPPING_TABLE
                + " WHERE LOWER(short_table_name) LIKE LOWER(?||'\\______%') ESCAPE '\\'"
                + " ORDER BY short_table_name DESC", truncatedTableName);
        return getTableNumberFromTableNames(truncatedTableName, results);
    }

    private int getTableNumberFromTableNames(String truncatedTableName, AgnosticResultSet results) {
        for (int i = 0; i < results.size(); i++) {
            String shortName = results.get(i).getString("short_table_name");
            try {
                return Integer.parseInt(shortName.substring(truncatedTableName.length() + 1)) + 1;
            } catch (NumberFormatException e) {
                //Table in different format - Do nothing;
            }
        }
        return 0;
    }
}
