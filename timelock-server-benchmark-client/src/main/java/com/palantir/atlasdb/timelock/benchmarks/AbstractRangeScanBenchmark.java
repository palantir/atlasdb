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

package com.palantir.atlasdb.timelock.benchmarks;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BenchmarksTableFactory;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.MetadataTable;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;

public abstract class AbstractRangeScanBenchmark extends AbstractBenchmark {

    private static final Logger log = LoggerFactory.getLogger(AbstractRangeScanBenchmark.class);

    private final SerializableTransactionManager txnManager;

    private final int dataSize;
    private final int numRows;

    protected volatile String bucket;
    protected final int batchSize;

    public AbstractRangeScanBenchmark(int numClients, int requestsPerClient,
            SerializableTransactionManager txnManager, int dataSize, int numRows) {
        super(numClients, requestsPerClient);
        this.txnManager = txnManager;
        this.bucket = UUID.randomUUID().toString();
        this.dataSize = dataSize;
        this.numRows = numRows;
        this.batchSize = Math.max(1, 9_000_000 / dataSize);
    }

    protected byte[] randomDataOfLength(int dataSize) {
        byte[] result = new byte[dataSize];
        ThreadLocalRandom.current().nextBytes(result);
        return result;
    }

    @Override
    public final void setup() {
        Optional<Metadata> existingMetadata = getMetadata();
        if (existingMetadata.isPresent()) {
            this.bucket = existingMetadata.get().bucket;
            log.info("found existing test data under bucket {}", bucket);
            return;
        }

        this.bucket = UUID.randomUUID().toString();
        log.info("creating new test data under bucket {}", bucket);

        writeData();

        putMetadata();
    }

    private void writeData() {
        int numWritten = 0;
        AtomicLong key = new AtomicLong(0);
        byte[] data = randomDataOfLength(dataSize);

        while (numWritten < numRows) {
            int numToWrite = Math.min(numRows - numWritten, batchSize);

            txnManager.runTaskWithRetry(txn -> {
                Map<Long, byte[]> values = Maps.newHashMap();
                for (int i = 0; i < numToWrite; i++) {
                    values.put(key.getAndIncrement(), data);
                }

                writeValues(txn, values);
                return null;
            });

            numWritten += numToWrite;
        }
    }

    @Override
    protected final void performOneCall() {
        List<byte[]> results = txnManager.runTaskReadOnly(txn -> getRange(txn, 0L, numRows));

        Preconditions.checkState(results.size() == numRows);
        for (byte[] resultData : results) {
            Preconditions.checkState(resultData.length == dataSize);
        }
    }

    protected abstract List<byte[]> getRange(Transaction txn, long startInclusive, long endExclusive);

    protected abstract void writeValues(Transaction txn, Map<Long, byte[]> valuesByKey);

    @Override
    public Map<String, Object> getExtraParameters() {
        return ImmutableMap.of("numRows", numRows, "dataSize", dataSize);
    }

    private final static class Metadata {

        @JsonProperty("bucket")
        String bucket;

        public Metadata() { }
        public Metadata(String bucket) {
            this.bucket = bucket;
        }
    }

    public static void main(String[] args) {
        Metadata metadata = new Metadata("foo");
        byte[] data = serialize(metadata);
        deserialize(data);
    }

    private void putMetadata() {
        txnManager.runTaskWithRetry(txn -> {
            MetadataTable table = BenchmarksTableFactory.of().getMetadataTable(txn);

            Metadata metadata = new Metadata(bucket);
            table.putData(MetadataTable.MetadataRow.of(getKeyForParameters()), serialize(metadata));
            return null;
        });
    }

    private static byte[] serialize(Metadata metadata) {
        try {
            return new ObjectMapper().writeValueAsBytes(metadata);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Optional<Metadata> getMetadata() {
        return txnManager.runTaskReadOnly(txn -> {
            MetadataTable table = BenchmarksTableFactory.of().getMetadataTable(txn);

            return table.getRow(MetadataTable.MetadataRow.of(getKeyForParameters()))
                    .map(MetadataTable.MetadataRowResult::getData)
                    .map(AbstractRangeScanBenchmark::deserialize);
        });
    }

    private static Metadata deserialize(byte[] blob) {
        try {
            return new ObjectMapper().readValue(blob, Metadata.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getKeyForParameters() {
        return getClass().getSimpleName() + "."
                + dataSize + "."
                + numRows;

    }
}
