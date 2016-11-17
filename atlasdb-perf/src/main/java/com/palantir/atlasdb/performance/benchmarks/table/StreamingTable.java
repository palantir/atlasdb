/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.performance.benchmarks.table;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Random;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

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

@State(Scope.Benchmark)
public class StreamingTable {
    private Random random = new Random(Tables.RANDOM_SEED);

    private AtlasDbServicesConnector connector;
    private AtlasDbServices services;

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
        this.connector.close();
    }

    @Setup(Level.Trial)
    public void setup(AtlasDbServicesConnector conn) {
        this.connector = conn;
        services = conn.connect();
        if (!services.getKeyValueService().getAllTableNames().contains(getTableRef())) {
            Schemas.createTablesAndIndexes(StreamTestSchema.getSchema(), getKvs());
            setupData();
        }
    }

    private void setupData() {
        StreamTestTableFactory tableFactory = StreamTestTableFactory.of();
        ValueStreamStore streamTestStreamStore = ValueStreamStore.of(
                getTransactionManager(),
                tableFactory
        );
        byte[] data = "bytes".getBytes(); //new byte[6_000_000];

//        random.nextBytes(data);

        InputStream inputStream = new ByteArrayInputStream(data);
        long streamId = streamTestStreamStore.storeStream(inputStream).getLhSide();

        getTransactionManager().runTaskThrowOnConflict(txn -> {
            KeyValueTable table = tableFactory.getKeyValueTable(txn);
            KeyValueTable.KeyValueRow row = KeyValueTable.KeyValueRow.of("row");
            KeyValueTable.StreamId id = KeyValueTable.StreamId.of(streamId);
            Multimap<Persistable, ColumnValue<?>> rows = ImmutableMultimap.of(row, id);

            txn.put(table.getTableRef(), ColumnValues.toCellValues(rows));
            return null;
        });

    }
}
