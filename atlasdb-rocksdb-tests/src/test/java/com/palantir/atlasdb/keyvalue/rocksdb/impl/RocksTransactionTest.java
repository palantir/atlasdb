/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.rocksdb.impl;

import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.impl.AbstractTransactionTest;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;

public class RocksTransactionTest extends AbstractTransactionTest {
    @Override
    protected KeyValueService getKeyValueService() {
        KeyValueService db = RocksDbKeyValueService.create("testdb");
        Set<TableReference> nonMetadataTables = db.getAllTableNames().stream()
                .filter(tableRef -> !tableRef.getNamespace().getName().equals("default")
                        || !tableRef.getTablename().equals("_metadata"))
                .collect(Collectors.toSet());
        db.dropTables(nonMetadataTables);
        return db;
    }

    @Override
    protected boolean supportsReverse() {
        return false;
    }

    @Override
    public void tearDown() {
        // This is implemented as a drop/create instead of a truncate() because of a actual issues
        // in the RocksDB KVS
        keyValueService.dropTables(ImmutableSet.of(TEST_TABLE, TransactionConstants.TRANSACTION_TABLE));
        keyValueService.createTables(ImmutableMap.of(
                TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA,
                TransactionConstants.TRANSACTION_TABLE, TransactionConstants.TRANSACTION_TABLE_METADATA.persistToBytes()));
    }
}
