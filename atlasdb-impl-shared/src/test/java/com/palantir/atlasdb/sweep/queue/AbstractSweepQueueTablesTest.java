/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.queue;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.tsPartitionFine;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.junit.Before;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.WriteReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;

public abstract class AbstractSweepQueueTablesTest {
    static final TableReference TABLE_CONS = TableReference.createFromFullyQualifiedName("test.conservative");
    static final TableReference TABLE_THOR = TableReference.createFromFullyQualifiedName("test.thorough");
    static final Cell DEFAULT_CELL = Cell.create(new byte[] {'r'}, new byte[] {'c'});
    static final long TS = 1_000_000_100L;
    static final long TS2 = 2 * TS;
    static final long TS_FINE_PARTITION = tsPartitionFine(TS);
    static final long TS2_FINE_PARTITION = tsPartitionFine(TS2);
    static final int DEFAULT_SHARDS = 128;
    static final int FIXED_SHARD = WriteInfo.write(TABLE_CONS, getCellWithFixedHash(0), 0L).toShard(DEFAULT_SHARDS);

    protected KeyValueService mockKvs = mock(KeyValueService.class);
    protected KeyValueService kvs;
    protected WriteInfoPartitioner partitioner;
    private TransactionService txnService;

    protected int numShards;

    @Before
    public void setup() {
        numShards = DEFAULT_SHARDS;
        when(mockKvs.getMetadataForTable(TABLE_CONS))
                .thenReturn(metadataBytes(TableMetadataPersistence.SweepStrategy.CONSERVATIVE));
        when(mockKvs.getMetadataForTable(TABLE_THOR))
                .thenReturn(metadataBytes(TableMetadataPersistence.SweepStrategy.THOROUGH));
        partitioner = new WriteInfoPartitioner(mockKvs, () -> numShards);
        kvs = spy(new InMemoryKeyValueService(true));
        txnService = TransactionServices.createTransactionService(kvs);
    }

    public static byte[] metadataBytes(TableMetadataPersistence.SweepStrategy sweepStrategy) {
        return new TableMetadata(new NameMetadataDescription(),
                new ColumnMetadataDescription(),
                ConflictHandler.RETRY_ON_WRITE_WRITE,
                TableMetadataPersistence.CachePriority.WARM,
                false,
                0,
                false,
                sweepStrategy,
                false,
                TableMetadataPersistence.LogSafety.UNSAFE)
                .persistToBytes();
    }

    public int writeToDefault(KvsSweepQueueWriter queueWriter, long timestamp, TableReference tableRef) {
        return writeToCell(queueWriter, timestamp, DEFAULT_CELL, tableRef);
    }

    public int writeToCell(KvsSweepQueueWriter queueWriter, long timestamp, Cell cell, TableReference tableRef) {
        putTimestampIntoTransactionTable(timestamp, Long.MAX_VALUE);
        return write(queueWriter, timestamp, cell, false, tableRef);
    }

    public int writeToCellUncommitted(KvsSweepQueueWriter queueWriter, long ts, Cell cell, TableReference tableRef) {
        return write(queueWriter, ts, cell, false, tableRef);
    }

    public int writeToCellAborted(KvsSweepQueueWriter queueWriter, long timestamp, Cell cell, TableReference tableRef) {
        putTimestampIntoTransactionTable(timestamp, TransactionConstants.FAILED_COMMIT_TS);
        return write(queueWriter, timestamp, cell, false, tableRef);
    }

    public int putTombstone(KvsSweepQueueWriter queueWriter, long timestamp, Cell cell, TableReference tableRef) {
        putTimestampIntoTransactionTable(timestamp, Long.MAX_VALUE);
        return write(queueWriter, timestamp, cell, true, tableRef);
    }

    public int putTombstoneUncommitted(KvsSweepQueueWriter queueWriter, long ts, Cell cell, TableReference tableRef) {
        return write(queueWriter, ts, cell, true, tableRef);
    }

    public int putTombstoneAborted(KvsSweepQueueWriter queueWriter, long ts, Cell cell, TableReference tableRef) {
        putTimestampIntoTransactionTable(ts, TransactionConstants.FAILED_COMMIT_TS);
        return write(queueWriter, ts, cell, true, tableRef);
    }

    private int write(KvsSweepQueueWriter queueWriter, long timestamp, Cell cell, boolean isTombstone,
            TableReference tableRef) {
        WriteInfo write = WriteInfo.of(WriteReference.of(tableRef, cell, isTombstone), timestamp);
        queueWriter.enqueue(ImmutableList.of(write));
        return write.toShard(numShards);
    }

    protected void putTimestampIntoTransactionTable(long ts, long commitTs) {
        try {
            txnService.putUnlessExists(ts, commitTs);
        } catch (KeyAlreadyExistsException e) {
            // this is fine
        }
    }

    public List<WriteInfo> writeToCellsInFixedShard(KvsSweepQueueWriter writer, long timestamp, int number,
            TableReference tableRef) {
        return writeToCellsInFixedShardStartWith(writer, timestamp, number, tableRef, 0L);
    }

    public List<WriteInfo> writeToCellsInFixedShardStartWith(KvsSweepQueueWriter writer, long timestamp,
            int number, TableReference tableRef, long startSeed) {
        List<WriteInfo> result = new ArrayList<>();
        for (long i = startSeed; i < startSeed + number; i++) {
            Cell cell = getCellWithFixedHash(i);
            result.add(WriteInfo.write(tableRef, cell, timestamp));
        }
        putTimestampIntoTransactionTable(timestamp, Long.MAX_VALUE);
        writer.enqueue(result);
        return result;
    }

    public static Cell getCellWithFixedHash(long seed) {
        return Cell.create(PtBytes.toBytes(seed), PtBytes.toBytes(seed));
    }

    public boolean isTransactionAborted(long txnTimestamp) {
        return Objects.equals(TransactionConstants.FAILED_COMMIT_TS, txnService.get(txnTimestamp));
    }
}
