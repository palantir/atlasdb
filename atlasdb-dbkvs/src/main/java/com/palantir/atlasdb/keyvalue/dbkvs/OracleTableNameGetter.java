/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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
import com.palantir.common.exception.TableMappingNotFoundException;
import java.util.Set;

public interface OracleTableNameGetter {
    String generateShortTableName(ConnectionSupplier connectionSupplier, TableReference tableRef);

    String generateShortOverflowTableName(ConnectionSupplier connectionSupplier, TableReference tableRef);

    String getInternalShortTableName(ConnectionSupplier connectionSupplier, TableReference tableRef)
            throws TableMappingNotFoundException;

    String getInternalShortOverflowTableName(ConnectionSupplier connectionSupplier, TableReference tableRef)
            throws TableMappingNotFoundException;

    Set<TableReference> getTableReferencesFromShortTableNames(
            ConnectionSupplier connectionSupplier, Set<String> shortTableNames);

    Set<TableReference> getTableReferencesFromShortOverflowTableNames(
            ConnectionSupplier connectionSupplier, Set<String> shortTableNames);

    String getPrefixedTableName(TableReference tableRef);

    String getPrefixedOverflowTableName(TableReference tableRef);

    void clearCacheForTable(String fullTableName);
}
