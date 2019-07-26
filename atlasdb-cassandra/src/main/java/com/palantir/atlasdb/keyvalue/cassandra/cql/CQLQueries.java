/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.cql;

import java.nio.ByteBuffer;
import java.util.List;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;

public class CQLQueries {

    private final SafeKeyspace keyspace;
    private final CqlFieldNameProvider fieldNameProvider = new CqlFieldNameProvider();

    public CQLQueries(SafeKeyspace keyspace) {
        this.keyspace = keyspace;
    }

    public Statement select(
            TableReference tableRef,
            byte[] column,
            List<Cell> batch,
            final long startTs,
            boolean loadAllTs,
            ConsistencyLevel consistency) {
        Select select = QueryBuilder.select()
                .from(keyspace.getQuoted(), tableName(tableRef));

        select.where(QueryBuilder.in(fieldNameProvider.row(),
                Iterables.transform(batch, cell -> ByteBuffer.wrap(cell.getRowName()))))
                .and(QueryBuilder.eq(fieldNameProvider.column(), ByteBuffer.wrap(column)))
                .and(QueryBuilder.gt(fieldNameProvider.timestamp(), ~startTs));

        if (!loadAllTs) {
            select.limit(1);
        }

        select.setConsistencyLevel(consistency);

        return select;
    }

    private String tableName(TableReference tableReference) {
        return '"' + AbstractKeyValueService.internalTableName(tableReference) + '"';
    }
}
