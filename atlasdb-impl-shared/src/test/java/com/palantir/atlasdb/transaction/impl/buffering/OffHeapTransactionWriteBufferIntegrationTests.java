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

package com.palantir.atlasdb.transaction.impl.buffering;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDB;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.local.storage.api.TransactionWriteBuffer;
import com.palantir.atlasdb.off.heap.rocksdb.RocksDbPersistentStore;

public final class OffHeapTransactionWriteBufferIntegrationTests {
    @ClassRule
    public static final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private static final TableReference DEFAULT_TABLE = TableReference.create(Namespace.DEFAULT_NAMESPACE, "test");
    private static final Cell DEFAULT_CELL = Cell.create("row".getBytes(), "column".getBytes());
    private static final byte[] DEFAULT_VALUE = "value".getBytes();

    private File testFolder;
    private RocksDB rocksDb;
    private TransactionWriteBuffer transactionWriteBuffer;

    @Before
    public void before() throws Exception {
        testFolder = temporaryFolder.newFolder();
        rocksDb = RocksDB.open(testFolder.getAbsolutePath());

        transactionWriteBuffer = OffHeapTransactionWriteBuffer.create(new RocksDbPersistentStore(rocksDb));
    }

    @After
    public void after() {
        rocksDb.close();
        testFolder.delete();
    }

    @Test
    public void correctlyStored() {
        transactionWriteBuffer.putWrites(DEFAULT_TABLE, ImmutableMap.of(DEFAULT_CELL, DEFAULT_VALUE));

        assertThat(transactionWriteBuffer.writesByTable(DEFAULT_TABLE))
                .containsExactly(Maps.immutableEntry(DEFAULT_CELL, DEFAULT_VALUE));
        assertThat(transactionWriteBuffer.writtenCells(DEFAULT_TABLE)).containsExactly(DEFAULT_CELL);
        assertThat(transactionWriteBuffer.hasWrites()).isTrue();
    }

    @Test
    public void overwritingDoesNotIncreaseWrittenBytes() {
        byte[] overwriting_value = "test1".getBytes();

        transactionWriteBuffer.putWrites(DEFAULT_TABLE, ImmutableMap.of(DEFAULT_CELL, DEFAULT_VALUE));
        long writtenBytes = transactionWriteBuffer.byteCount();
        transactionWriteBuffer.putWrites(DEFAULT_TABLE, ImmutableMap.of(DEFAULT_CELL, overwriting_value));

        assertThat(transactionWriteBuffer.writesByTable(DEFAULT_TABLE))
                .containsExactly(Maps.immutableEntry(DEFAULT_CELL, overwriting_value));
        assertThat(transactionWriteBuffer.writtenCells(DEFAULT_TABLE)).containsExactly(DEFAULT_CELL);
        assertThat(transactionWriteBuffer.hasWrites()).isTrue();
        assertThat(transactionWriteBuffer.byteCount()).isEqualTo(writtenBytes);
    }
}
