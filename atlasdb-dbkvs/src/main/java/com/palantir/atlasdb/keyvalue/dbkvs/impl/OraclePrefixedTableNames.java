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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import com.google.common.base.Throwables;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.DdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.impl.TableMappingNotFoundException;

public class OraclePrefixedTableNames extends PrefixedTableNames {
    private OracleTableNameGetter oracleTableNameGetter;
    private ConnectionSupplier connectionSupplier;

    public OraclePrefixedTableNames(
            DdlConfig config,
            ConnectionSupplier connectionSupplier,
            OracleTableNameGetter oracleTableNameGetter) {
        super(config);
        this.connectionSupplier = connectionSupplier;
        this.oracleTableNameGetter = oracleTableNameGetter;
    }

    @Override
    public String get(TableReference tableRef) {
        try {
            return oracleTableNameGetter.getInternalShortTableName(connectionSupplier, tableRef);
        } catch (TableMappingNotFoundException e) {
            throw Throwables.propagate(e);
        }
    }
}
