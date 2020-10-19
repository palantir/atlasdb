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
package com.palantir.atlasdb.timelock.benchmarks.benchmarks;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BenchmarksTableFactory;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.MetadataTable;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.random.RandomBytes;
import com.palantir.logsafe.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A base class for implementing range scan benchmarks. A primary function of this class is to store metadata
 * about previous data that has been written, so that multiple benchmarks with the same parameters will just
 * read the same data rather than writing new data for each benchmarks. Besides optimizing the time taken, this
 * allows us to run compactions on the KVS before reading the data.
 */
public abstract class AbstractRangeScanBenchmark extends AbstractBenchmark {

    private static final Logger log = LoggerFactory.getLogger(AbstractRangeScanBenchmark.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final TransactionManager txnManager;

    private static final int MAX_BYTES_PER_WRITE = (int) (10_000_000 * 0.9);

    private final int dataSize;
    private final int numRows;

    protected volatile String bucket;
    protected final int batchSize;

    public AbstractRangeScanBenchmark(int numClients, int requestsPerClient,
            TransactionManager txnManager, int dataSize, int numRows) {
        super(numClients, requestsPerClient);
        this.txnManager = txnManager;
        this.dataSize = dataSize;
        this.numRows = numRows;
        this.batchSize = Math.max(1, MAX_BYTES_PER_WRITE / dataSize);
    }

    @Override
    public final void setup() {
        Optional<String> existingBucket = getExistingBucketForParameters();
        if (existingBucket.isPresent()) {
            this.bucket = existingBucket.get();
            log.info("found existing test data under bucket {}", bucket);
            return;
        }

        this.bucket = UUID.randomUUID().toString();
        log.info("creating new test data under bucket {}", bucket);

        writeData();

        recordMetadata();
    }

    private void writeData() {
        int numWritten = 0;
        AtomicLong key = new AtomicLong(0);
        byte[] data = RandomBytes.ofLength(dataSize);

        while (numWritten < numRows) {
            int numToWrite = Math.min(numRows - numWritten, batchSize);

            txnManager.runTaskWithRetry(txn -> {
                Map<Long, byte[]> values = Maps.newHashMapWithExpectedSize(numToWrite);
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

    public static void main(String[] args) {
        Metadata metadata = new Metadata("foo");
        byte[] data = serialize(metadata);
        deserialize(data);
    }

    private void recordMetadata() {
        txnManager.runTaskWithRetry(txn -> {
            MetadataTable table = BenchmarksTableFactory.of().getMetadataTable(txn);

            Metadata metadata = new Metadata(bucket);
            table.putData(MetadataTable.MetadataRow.of(getKeyForParameters()), serialize(metadata));
            return null;
        });
    }

    private Optional<String> getExistingBucketForParameters() {
        return txnManager.runTaskReadOnly(txn -> {
            MetadataTable table = BenchmarksTableFactory.of().getMetadataTable(txn);

            return table.getRow(MetadataTable.MetadataRow.of(getKeyForParameters()))
                    .map(MetadataTable.MetadataRowResult::getData)
                    .map(AbstractRangeScanBenchmark::deserialize)
                    .map(mdata -> mdata.bucket);
        });
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
        return getClass().getSimpleName() + "."
                + dataSize + "."
                + numRows;

    }

    private static final class Metadata {

        @JsonProperty("bucket")
        String bucket;

        Metadata(@JsonProperty("bucket") String bucket) {
            this.bucket = bucket;
        }
    }
}
