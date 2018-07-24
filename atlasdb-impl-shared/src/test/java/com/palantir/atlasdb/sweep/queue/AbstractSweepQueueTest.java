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

import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.assertj.core.api.Assertions;
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
import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;

public abstract class AbstractSweepQueueTest {
    static final TableReference TABLE_CONS = TableReference.createFromFullyQualifiedName("test.conservative");
    static final TableReference TABLE_THOR = TableReference.createFromFullyQualifiedName("test.thorough");
    static final TableReference TABLE_NOTH = TableReference.createFromFullyQualifiedName("test.nothing");
    static final Cell DEFAULT_CELL = Cell.create(new byte[] {'r'}, new byte[] {'c'});
    static final long TS = SweepQueueUtils.TS_COARSE_GRANULARITY + 100L;
    static final long TS2 = 2 * TS;
    static final long TS_FINE_PARTITION = SweepQueueUtils.tsPartitionFine(TS);
    static final long TS2_FINE_PARTITION = SweepQueueUtils.tsPartitionFine(TS2);
    static final int DEFAULT_SHARDS = 8;
    static final int FIXED_SHARD = WriteInfo.write(TABLE_CONS, getCellWithFixedHash(0), 0L).toShard(DEFAULT_SHARDS);
    static final int CONS_SHARD = WriteInfo.tombstone(TABLE_CONS, DEFAULT_CELL, 0).toShard(DEFAULT_SHARDS);
    static final int THOR_SHARD = WriteInfo.tombstone(TABLE_THOR, DEFAULT_CELL, 0).toShard(DEFAULT_SHARDS);

    int numShards;
    long immutableTs;
    long unreadableTs;
    int shardCons;
    int shardThor;

    protected MetricsManager metricsManager;

    KeyValueService spiedKvs;
    SpecialTimestampsSupplier timestampsSupplier;
    WriteInfoPartitioner partitioner;
    TransactionService txnService;

    @Before
    public void setup() {
        numShards = DEFAULT_SHARDS;
        unreadableTs = SweepQueueUtils.TS_COARSE_GRANULARITY * 5;
        immutableTs = SweepQueueUtils.TS_COARSE_GRANULARITY * 5;

        metricsManager = MetricsManagers.createForTests();
        spiedKvs = spy(new InMemoryKeyValueService(true));
        spiedKvs.createTable(TABLE_CONS, metadataBytes(TableMetadataPersistence.SweepStrategy.CONSERVATIVE));
        spiedKvs.createTable(TABLE_THOR, metadataBytes(TableMetadataPersistence.SweepStrategy.THOROUGH));
        spiedKvs.createTable(TABLE_NOTH, metadataBytes(TableMetadataPersistence.SweepStrategy.NOTHING));
        timestampsSupplier = new SpecialTimestampsSupplier(() -> unreadableTs, () -> immutableTs);
        partitioner = new WriteInfoPartitioner(spiedKvs, () -> numShards);
        txnService = TransactionServices.createTransactionService(spiedKvs);
    }

    static byte[] metadataBytes(TableMetadataPersistence.SweepStrategy sweepStrategy) {
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

    int writeToDefaultCellCommitted(KvsSweepQueueWriter queueWriter, long timestamp, TableReference tableRef) {
        return writeToCellCommitted(queueWriter, timestamp, DEFAULT_CELL, tableRef);
    }

    int writeToCellCommitted(KvsSweepQueueWriter queueWriter, long timestamp, Cell cell, TableReference tableRef) {
        putTimestampIntoTransactionTable(timestamp, timestamp);
        return write(queueWriter, timestamp, cell, false, tableRef);
    }

    int writeToDefaultCellUncommitted(KvsSweepQueueWriter queueWriter, long ts, TableReference tableRef) {
        return write(queueWriter, ts, DEFAULT_CELL, false, tableRef);
    }

    int writeToDefaultCellAborted(KvsSweepQueueWriter queueWriter, long timestamp, TableReference tableRef) {
        putTimestampIntoTransactionTable(timestamp, TransactionConstants.FAILED_COMMIT_TS);
        return write(queueWriter, timestamp, DEFAULT_CELL, false, tableRef);
    }

    int writeToDefaultCellCommitedAt(KvsSweepQueueWriter queueWriter, long startTs, long commitTs,
            TableReference tableRef) {
        putTimestampIntoTransactionTable(startTs, commitTs);
        return write(queueWriter, startTs, DEFAULT_CELL, false, tableRef);
    }

    int putTombstoneToDefaultCommitted(KvsSweepQueueWriter queueWriter, long timestamp, TableReference tableRef) {
        putTimestampIntoTransactionTable(timestamp, timestamp);
        return write(queueWriter, timestamp, DEFAULT_CELL, true, tableRef);
    }

    private int write(KvsSweepQueueWriter writer, long ts, Cell cell, boolean isTombstone, TableReference tableRef) {
        WriteInfo write = WriteInfo.of(WriteReference.of(tableRef, cell, isTombstone), ts);
        writer.enqueue(ImmutableList.of(write));
        return write.toShard(numShards);
    }

    void putTimestampIntoTransactionTable(long ts, long commitTs) {
        try {
            txnService.putUnlessExists(ts, commitTs);
        } catch (KeyAlreadyExistsException e) {
            // this is fine if the existing key is what we wanted
            Assertions.assertThat(txnService.get(ts)).isEqualTo(commitTs);
        }
    }

    List<WriteInfo> writeToCellsInFixedShard(KvsSweepQueueWriter writer, long ts, int number, TableReference tableRef) {
        List<WriteInfo> result = new ArrayList<>();
        for (long i = 0; i < number; i++) {
            Cell cell = getCellWithFixedHash(i);
            result.add(WriteInfo.write(tableRef, cell, ts));
        }
        putTimestampIntoTransactionTable(ts, ts);
        writer.enqueue(result);
        return result;
    }

    static Cell getCellWithFixedHash(long seed) {
        return Cell.create(PtBytes.toBytes(seed), PtBytes.toBytes(seed));
    }

    boolean isTransactionAborted(long txnTimestamp) {
        return Objects.equals(TransactionConstants.FAILED_COMMIT_TS, txnService.get(txnTimestamp));
    }

    long getSweepTsCons() {
        return Sweeper.CONSERVATIVE.getSweepTimestamp(timestampsSupplier);
    }

    void runWithConservativeSweepTimestamp(Runnable runnable, long desiredSweepTimestamp) {
        immutableTs = desiredSweepTimestamp;
        unreadableTs = desiredSweepTimestamp;
        runnable.run();
    }
}
