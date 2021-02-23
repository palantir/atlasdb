/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.sweep.queue;

import static org.mockito.Mockito.spy;

import com.google.common.collect.ImmutableList;
import com.google.common.math.IntMath;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.WriteReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.atlasdb.sweep.queue.clear.SafeTableClearerKeyValueService;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;

public abstract class AbstractSweepQueueTest {
    static final TableReference TABLE_CONS = TableReference.createFromFullyQualifiedName("test.conservative");
    static final TableReference TABLE_THOR = TableReference.createFromFullyQualifiedName("test.thorough");
    static final TableReference TABLE_THOR_MIGRATION =
            TableReference.createFromFullyQualifiedName("test.thoroughmigration");
    static final TableReference TABLE_NOTH = TableReference.createFromFullyQualifiedName("test.nothing");
    static final Cell DEFAULT_CELL = Cell.create(new byte[] {'r'}, new byte[] {'c'});
    static final long TS = SweepQueueUtils.TS_COARSE_GRANULARITY + 100L;
    static final long TS2 = 2 * TS;
    static final long TS_FINE_PARTITION = SweepQueueUtils.tsPartitionFine(TS);
    static final long TS2_FINE_PARTITION = SweepQueueUtils.tsPartitionFine(TS2);
    static final int DEFAULT_SHARDS = 8;
    static final int FIXED_SHARD = WriteInfo.write(
                    TABLE_CONS,
                    getCellRefWithFixedShard(0, TABLE_CONS, DEFAULT_SHARDS).cell(),
                    0L)
            .toShard(DEFAULT_SHARDS);
    static final int CONS_SHARD =
            WriteInfo.tombstone(TABLE_CONS, DEFAULT_CELL, 0).toShard(DEFAULT_SHARDS);
    static final int THOR_SHARD =
            WriteInfo.tombstone(TABLE_THOR, DEFAULT_CELL, 0).toShard(DEFAULT_SHARDS);
    static final int THOR_MIGRATION_SHARD =
            WriteInfo.tombstone(TABLE_THOR_MIGRATION, DEFAULT_CELL, 0).toShard(DEFAULT_SHARDS);

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
        timestampsSupplier = new SpecialTimestampsSupplier(() -> unreadableTs, () -> immutableTs);
        spiedKvs = spy(new SafeTableClearerKeyValueService(
                timestampsSupplier::getImmutableTimestamp, new InMemoryKeyValueService(true)));
        spiedKvs.createTable(TABLE_CONS, metadataBytes(SweepStrategy.CONSERVATIVE));
        spiedKvs.createTable(TABLE_THOR, metadataBytes(SweepStrategy.THOROUGH));
        spiedKvs.createTable(TABLE_THOR_MIGRATION, metadataBytes(SweepStrategy.THOROUGH_MIGRATION));
        spiedKvs.createTable(TABLE_NOTH, metadataBytes(SweepStrategy.NOTHING));
        partitioner = new WriteInfoPartitioner(spiedKvs, () -> numShards);
        txnService = TransactionServices.createV1TransactionService(spiedKvs);
    }

    @After
    public void tearDown() {
        // This is required because of JUnit memory issues
        spiedKvs = null;
    }

    static byte[] metadataBytes(SweepStrategy sweepStrategy) {
        return TableMetadata.builder().sweepStrategy(sweepStrategy).build().persistToBytes();
    }

    int writeToDefaultCellCommitted(SweepQueueTable queueWriter, long timestamp, TableReference tableRef) {
        return writeToCellCommitted(queueWriter, timestamp, DEFAULT_CELL, tableRef);
    }

    int writeToCellCommitted(SweepQueueTable queueWriter, long timestamp, Cell cell, TableReference tableRef) {
        putTimestampIntoTransactionTable(timestamp, timestamp);
        return write(queueWriter, timestamp, cell, false, tableRef);
    }

    int writeToDefaultCellUncommitted(SweepQueueTable queueWriter, long ts, TableReference tableRef) {
        return write(queueWriter, ts, DEFAULT_CELL, false, tableRef);
    }

    int writeToDefaultCellAborted(SweepQueueTable queueWriter, long timestamp, TableReference tableRef) {
        putTimestampIntoTransactionTable(timestamp, TransactionConstants.FAILED_COMMIT_TS);
        return write(queueWriter, timestamp, DEFAULT_CELL, false, tableRef);
    }

    int writeToDefaultCellCommitedAt(
            SweepQueueTable queueWriter, long startTs, long commitTs, TableReference tableRef) {
        putTimestampIntoTransactionTable(startTs, commitTs);
        return write(queueWriter, startTs, DEFAULT_CELL, false, tableRef);
    }

    int putTombstoneToDefaultCommitted(SweepQueueTable queueWriter, long timestamp, TableReference tableRef) {
        putTimestampIntoTransactionTable(timestamp, timestamp);
        return write(queueWriter, timestamp, DEFAULT_CELL, true, tableRef);
    }

    private int write(SweepQueueTable writer, long ts, Cell cell, boolean isTombstone, TableReference tableRef) {
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

    List<WriteInfo> writeToCellsInFixedShard(SweepQueueTable writer, long ts, int number, TableReference tableRef) {
        List<WriteInfo> result = new ArrayList<>();
        for (long i = 0; i < number; i++) {
            CellReference cellRef = getCellRefWithFixedShard(i, tableRef, numShards);
            result.add(WriteInfo.write(tableRef, cellRef.cell(), ts));
        }
        putTimestampIntoTransactionTable(ts, ts);
        writer.enqueue(result);
        return result;
    }

    static CellReference getCellRefWithFixedShard(long seed, TableReference tableRef, int shards) {
        byte[] rowName = PtBytes.toBytes(seed);

        return IntStream.iterate(0, i -> i + 1)
                .mapToObj(index -> CellReference.of(tableRef, Cell.create(rowName, PtBytes.toBytes(index))))
                .filter(cellReference -> IntMath.mod(cellReference.goodHash(), shards) == 0)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Infinite stream had no cell possibilities :("));
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
