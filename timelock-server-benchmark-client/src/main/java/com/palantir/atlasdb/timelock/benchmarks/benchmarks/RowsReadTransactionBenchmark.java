/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.benchmarks.benchmarks;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.timelock.benchmarks.RandomBytes;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BenchmarksTableFactory;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BlobsTable;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BlobsTable.BlobsRow;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.KvRowsTable;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.MetadataTable;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;

public final class RowsReadTransactionBenchmark extends AbstractBenchmark {

    private static final Logger log = LoggerFactory.getLogger(RowsReadTransactionBenchmark.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final TransactionManager txnManager;

    private final int dataSize;
    private final int numRows;
    private volatile List<byte[]> keys;
    private final int numUpdatesPerCell;

    public static Map<String, Object> execute(SerializableTransactionManager txnManager, int numClients,
            int requestsPerClient, int numRows, int dataSize, int numUpdatesPerCell) {
        return new RowsReadTransactionBenchmark(txnManager, numClients, requestsPerClient, numRows, dataSize, numUpdatesPerCell).execute();
    }

    private RowsReadTransactionBenchmark(TransactionManager txnManager, int numClients, int requestsPerClient,
            int numRows, int dataSize, int numUpdatesPerCell) {
        super(numClients, requestsPerClient);
        this.txnManager = txnManager;

        this.dataSize = dataSize;
        this.numRows = numRows;
        this.numUpdatesPerCell = numUpdatesPerCell;
    }

    @Override
    public final void setup() {
        Optional<List<byte[]>> existingBucket = getExistingKeysForParameters();
        if (existingBucket.isPresent()) {
            this.keys = existingBucket.get();
            log.info("found existing test data");
            log.info("first key is {}", PtBytes.encodeHexString(BlobsRow.of(keys.get(0)).persistToBytes()));
            return;
        }

        this.keys = IntStream.range(0, numRows)
                .mapToObj(i -> RandomBytes.ofLength(16))
                .collect(Collectors.toList());
        log.info("creating new test data");

        writeData();

        recordMetadata();
    }

    private void writeData() {
        for (int i = 0 ; i < numUpdatesPerCell + 1; i++) {
            txnManager.runTaskWithRetry(txn -> {
                BlobsTable table = BenchmarksTableFactory.of().getBlobsTable(txn);
                for (byte[] key : keys) {
                    table.putData(BlobsRow.of(key), RandomBytes.ofLength(dataSize));
                }
                return null;
            });
        }
    }

    private void recordMetadata() {
        txnManager.runTaskWithRetry(txn -> {
            MetadataTable table = BenchmarksTableFactory.of().getMetadataTable(txn);

            Metadata metadata = new Metadata(keys);
            table.putData(MetadataTable.MetadataRow.of(getKeyForParameters()), serialize(metadata));
            return null;
        });
    }

    @Override
    public void performOneCall() {
        List<byte[]> result = txnManager.runTaskReadOnly(txn -> {
            BlobsTable table = BenchmarksTableFactory.of().getBlobsTable(txn);
            List<BlobsRow> rowKeys = keys.stream().map(BlobsRow::of).collect(Collectors.toList());

            return table.getRows(rowKeys, KvRowsTable.getColumnSelection(KvRowsTable.KvRowsNamedColumn.DATA)).stream()
                    .map(BlobsTable.BlobsRowResult::getData)
                    .collect(Collectors.toList());
        });

        Preconditions.checkState(result.size() == keys.size());
    }

    private Optional<List<byte[]>> getExistingKeysForParameters() {
        return txnManager.runTaskReadOnly(txn -> {
            MetadataTable table = BenchmarksTableFactory.of().getMetadataTable(txn);

            return table.getRow(MetadataTable.MetadataRow.of(getKeyForParameters()))
                    .map(MetadataTable.MetadataRowResult::getData)
                    .map(RowsReadTransactionBenchmark::deserialize)
                    .map(mdata -> mdata.keys);
        });
    }


    @Override
    public Map<String, Object> getExtraParameters() {
        return ImmutableMap.of("numRows", numRows, "dataSize", dataSize, "numUpdatesPerCell", numUpdatesPerCell);
    }

    private static byte[] serialize(Metadata metadata) {
        try {
            return MAPPER.writeValueAsBytes(metadata);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Metadata deserialize(byte[] blob) {
        try {
            return MAPPER.readValue(blob, Metadata.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getKeyForParameters() {
        return "v2."
                + getClass().getSimpleName() + "."
                + dataSize + "."
                + numRows + "."
                + numUpdatesPerCell;

    }

    private static final class Metadata {

        @JsonProperty("keys")
        List<byte[]> keys;

        Metadata(@JsonProperty("keys") List<byte[]> keys) {
            this.keys = keys;
        }
    }

}
