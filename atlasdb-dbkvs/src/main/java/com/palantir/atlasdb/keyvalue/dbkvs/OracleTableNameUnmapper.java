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

import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.atlasdb.keyvalue.impl.TableMappingNotFoundException;
import com.palantir.nexus.db.sql.AgnosticResultSet;

class OracleTableNameUnmapper {
    private final ConnectionSupplier conns;

    private LoadingCache<String, String> unmappingCache = CacheBuilder.newBuilder().build(
            new CacheLoader<String, String>() {
                @Override
                public String load(String fullTableName) throws Exception {
                    AgnosticResultSet results = conns.get().selectResultSetUnregisteredQuery(
                            "SELECT short_table_name "
                                    + "FROM " + AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE
                                    + " WHERE table_name = ?", fullTableName);
                    if (results.size() == 0) {
                        throw new TableMappingNotFoundException(
                                "The table " + fullTableName + " does not have a mapping."
                                        + "This might be because the table does not exist.");
                    }
                    return Iterables.getOnlyElement(results.rows()).getString("short_table_name");
                }
            }
    );

    OracleTableNameUnmapper(ConnectionSupplier conns) {
        this.conns = conns;
    }

    public String getShortTableNameFromMappingTable(String tablePrefix, TableReference tableRef)
            throws TableMappingNotFoundException {
        String fullTableName = tablePrefix + DbKvs.internalTableName(tableRef);
        try {
            return unmappingCache.get(fullTableName);
        } catch (ExecutionException e) {
            throw new TableMappingNotFoundException(e);
        }
    }
}
