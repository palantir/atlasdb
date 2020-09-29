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
package com.palantir.atlasdb.performance.benchmarks.table;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.performance.backend.AtlasDbServicesConnector;
import com.palantir.atlasdb.performance.schema.StreamTestSchema;
import com.palantir.atlasdb.performance.schema.generated.KeyValueTable;
import com.palantir.atlasdb.performance.schema.generated.StreamTestTableFactory;
import com.palantir.atlasdb.performance.schema.generated.ValueStreamStore;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.table.api.ColumnValue;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.table.generation.ColumnValues;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.persist.Persistable;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Benchmark)
public class StreamingTable {
    private Random random = new Random(Tables.RANDOM_SEED);

    private AtlasDbServicesConnector connector;
    private AtlasDbServices services;

    private long smallStreamId;
    private long largeStreamId;
    private long veryLargeStreamId;
    private byte[] largeStreamFirstBytes;
    private byte[] veryLargeStreamFirstBytes;

    public long getSmallStreamId() {
        return smallStreamId;
    }

    public long getLargeStreamId() {
        return largeStreamId;
    }

    public long getVeryLargeStreamId() {
        return veryLargeStreamId;
    }

    public byte[] getLargeStreamFirstBytes() {
        return largeStreamFirstBytes;
    }

    public byte[] getVeryLargeStreamFirstBytes() {
        return veryLargeStreamFirstBytes;
    }

    public TransactionManager getTransactionManager() {
        return services.getTransactionManager();
    }

    public KeyValueService getKvs() {
        return services.getKeyValueService();
    }

    public TableReference getTableRef() {
        Namespace namespace = Namespace.create("default", Namespace.UNCHECKED_NAME);
        String tableName = "blobs";
        return TableReference.create(namespace, tableName);
    }

    @TearDown(Level.Trial)
    public void cleanup() throws Exception {
        services.getKeyValueService().dropTable(getTableRef());
        this.connector.close();
    }

    @Setup(Level.Trial)
    public void setup(AtlasDbServicesConnector conn) {
        this.connector = conn;
        this.services = conn.connect();
        Schemas.createTablesAndIndexes(StreamTestSchema.getSchema(), getKvs());
        setupData();
    }

    private void setupData() {
        // Short streamable data
        byte[] data = "bytes".getBytes(StandardCharsets.UTF_8);
        smallStreamId = storeStreamForRow(data, "row");

        // Long streamable data
        byte[] randomData = new byte[6_000_000];
        random.nextBytes(randomData);
        largeStreamId = storeStreamForRow(randomData, "row2");
        largeStreamFirstBytes = Arrays.copyOf(randomData, 16);

        // Longer streamable data
        byte[] bigRandomData = new byte[60_000_000];
        random.nextBytes(bigRandomData);
        veryLargeStreamId = storeStreamForRow(bigRandomData, "row3");
        veryLargeStreamFirstBytes = Arrays.copyOf(bigRandomData, 16);
    }

    private Long storeStreamForRow(byte[] data, String rowName) {
        StreamTestTableFactory tableFactory = StreamTestTableFactory.of();
        ValueStreamStore streamTestStreamStore = ValueStreamStore.of(
                getTransactionManager(),
                tableFactory
        );

        final Long streamId = streamTestStreamStore.storeStream(new ByteArrayInputStream(data)).getLhSide();

        getTransactionManager().runTaskThrowOnConflict(txn -> {
            KeyValueTable table = tableFactory.getKeyValueTable(txn);
            KeyValueTable.KeyValueRow row = KeyValueTable.KeyValueRow.of(rowName);
            KeyValueTable.StreamId id = KeyValueTable.StreamId.of(streamId);
            Multimap<Persistable, ColumnValue<?>> rows = ImmutableMultimap.of(row, id);

            txn.put(table.getTableRef(), ColumnValues.toCellValues(rows));
            return null;
        });
        return streamId;
    }
}
