/*
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.DdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.PrefixedTableNames;

public class PostgresPrefixedTableNames implements PrefixedTableNames {

    private final DdlConfig config;

    public PostgresPrefixedTableNames(DdlConfig config) {
        this.config = config;
    }

    @Override
    public String get(TableReference tableRef, ConnectionSupplier connectionSupplier) {
        return get(tableRef);
    }

    public String get(TableReference tableRef) {
        return config.tablePrefix() + DbKvs.internalTableName(tableRef);
    }
}
