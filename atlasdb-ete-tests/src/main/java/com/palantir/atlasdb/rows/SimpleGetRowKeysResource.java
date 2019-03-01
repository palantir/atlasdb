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

package com.palantir.atlasdb.rows;

import java.util.List;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;

public class SimpleGetRowKeysResource implements GetRowKeysResource {
    private static final TableReference TABLE_REFERENCE = TableReference.createFromFullyQualifiedName("test.getRows");
    private static final byte[] BYTES = new byte[] {1, 2, 3};

    private final CassandraKeyValueService kvs;

    public SimpleGetRowKeysResource(KeyValueService wrappedKvs) {
        this.kvs = unwrapCassandraKvs(wrappedKvs);
    }

    @Override
    public void resetTable() {
        kvs.createTable(TABLE_REFERENCE,
                AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.truncateTable(TABLE_REFERENCE);
    }

    @Override
    public void insertCell(Cell cell) {
        kvs.put(TABLE_REFERENCE, ImmutableMap.of(cell, BYTES), 1L);
    }

    @Override
    public List<byte[]> getRowKeys(int startRow, int maxRows) {
        return kvs.getRowKeysInRange(TABLE_REFERENCE, PtBytes.toBytes(startRow), maxRows);
    }

    private static CassandraKeyValueService unwrapCassandraKvs(KeyValueService wrappedKvs) {
        KeyValueService keyValueService = wrappedKvs;
        while (!(keyValueService instanceof CassandraKeyValueService)) {
            keyValueService = getDelegate(keyValueService);
        }
        return (CassandraKeyValueService) keyValueService;
    }

    private static KeyValueService getDelegate(KeyValueService keyValueService) {
        return keyValueService.getDelegates().iterator().next();
    }
}
