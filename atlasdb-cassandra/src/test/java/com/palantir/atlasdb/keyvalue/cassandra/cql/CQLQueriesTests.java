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

import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public final class CQLQueriesTests {

    private final SafeKeyspace keyspace = SafeKeyspace.of("keyspace");
    private final TableReference tableReference = TableReference.create(Namespace.DEFAULT_NAMESPACE, "table");
    private final byte[] column = PtBytes.toBytes("col");
    private final byte[] row = PtBytes.toBytes("row");
    private final ImmutableList<Cell> cells = ImmutableList.of(Cell.create(row, column));
    private final CQLQueries queries = new CQLQueries(keyspace);

    @Test
    public void testSelect() {
        queries.select(tableReference, column, cells, 123L, true, ConsistencyLevel.LOCAL_QUORUM);
    }
}
