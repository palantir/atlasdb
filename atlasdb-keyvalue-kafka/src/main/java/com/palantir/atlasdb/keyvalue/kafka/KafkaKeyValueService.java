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
package com.palantir.atlasdb.keyvalue.kafka;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.kafka.KafkaKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public class KafkaKeyValueService implements KeyValueService {
    private static final Logger log = LoggerFactory.getLogger(KafkaKeyValueService.class);

    private static final String OPERATIONS_TOPIC = "operations";

    private final KafkaKeyValueServiceConfig config;
    private final KafkaProducer<CommandType, TaggedArray> producer;

    private final ConcurrentHashMap<Tag, BiConsumer<KeyValueService, Optional<Exception>>> operationsToPerform =
            new ConcurrentHashMap<>();

    private volatile boolean isClosed = false;

    public KafkaKeyValueService(KafkaKeyValueServiceConfig config) {
        this.config = config;
        this.producer = new KafkaProducer<>(ImmutableMap.<String, Object>builder()
                .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
                .put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString())
                .put(ProducerConfig.BATCH_SIZE_CONFIG, 1)
                .put(ProducerConfig.ACKS_CONFIG, "all")
                .put(ProducerConfig.LINGER_MS_CONFIG, 0)
                .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, CommandTypeSerialiser.class.getCanonicalName())
                .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TaggedArraySerialiser.class.getCanonicalName())
                .build());
    }

    public static KeyValueService create(KafkaKeyValueServiceConfig config) {
        KafkaKeyValueService kvs = new KafkaKeyValueService(config);
        kvs.startConsumerThread();
        return kvs;
    }

    private void startConsumerThread() {
        Thread consumerThread = new Thread(() -> {
            ExecutorService executor = PTExecutors.newCachedThreadPool();
            KafkaConsumer<CommandType, TaggedArray> consumer = createNewKafkaConsumer();
            consumer.subscribe(ImmutableSet.of(OPERATIONS_TOPIC));
            try {
                InMemoryKeyValueService kvs = new InMemoryKeyValueService(false);
                while (true) {
                    ConsumerRecords<CommandType, TaggedArray> records = consumer.poll(10000);
                    for (ConsumerRecord<CommandType, TaggedArray> record : records) {
                        CommandType commandType = record.key();
                        Tag tag = record.value().getTag();
                        byte[] data = record.value().getData();

                        Optional<Exception> exceptionThrown = Optional.empty();
                        try {
                            commandType.runOperation(kvs, data);
                        } catch (Exception e) {
                            exceptionThrown = Optional.of(e);
                        }

                        if (operationsToPerform.containsKey(tag)) {
                            Optional<Exception> exceptionToPassThrough = exceptionThrown;
                            executor.submit(() -> operationsToPerform.remove(tag).accept(kvs, exceptionToPassThrough));
                        }
                    }
                    if (isClosed) {
                        return;
                    }
                    consumer.commitSync();
                }
            } catch (Throwable t) {
                log.error("Exception thrown while processing the database queue!", t);
            } finally {
                executor.shutdownNow();
                consumer.close();
            }
        });
        consumerThread.setDaemon(true);
        consumerThread.start();
    }

    @Override
    public void createTable(TableReference tableRef, byte[] tableMetadata) throws InsufficientConsistencyException {
        createTables(ImmutableMap.of(tableRef, tableMetadata));
    }

    @Override
    public void createTables(Map<TableReference, byte[]> tableRefToTableMetadata)
            throws InsufficientConsistencyException {
        queueOperation(
                CommandType.CREATE_TABLES,
                CommandTypeSerialisers.createTables(tableRefToTableMetadata))
                .getResult();
    }

    @Override
    public void truncateTable(TableReference tableRef) throws InsufficientConsistencyException {
        truncateTables(ImmutableSet.of(tableRef));
    }

    @Override
    public void truncateTables(Set<TableReference> tableRefs) throws InsufficientConsistencyException {
        queueOperation(
                CommandType.TRUNCATE_TABLES,
                CommandTypeSerialisers.truncateTables(tableRefs))
                .getResult();
    }

    @Override
    public void dropTable(TableReference tableRef) throws InsufficientConsistencyException {
        dropTables(ImmutableSet.of(tableRef));
    }

    @Override
    public void dropTables(Set<TableReference> tableRefs) throws InsufficientConsistencyException {
        queueOperation(
                CommandType.DROP_TABLES,
                CommandTypeSerialisers.dropTables(tableRefs))
                .getResult();
    }

    @Override
    public synchronized void close() {
        if (!isClosed) {
            isClosed = true;
            producer.close();
        }
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return ImmutableSet.of();
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp)
            throws KeyAlreadyExistsException {
        ImmutableListMultimap.Builder<Cell, Value> valuesWithTimestamp = ImmutableListMultimap.builder();
        values.forEach((cell, contents) -> valuesWithTimestamp.put(cell, Value.create(contents, timestamp)));
        putWithTimestamps(tableRef, valuesWithTimestamp.build());
    }

    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp)
            throws KeyAlreadyExistsException {
        queueOperation(
                CommandType.MULTI_PUT,
                CommandTypeSerialisers.multiPut(valuesByTable, timestamp))
                .getResultWithKeyAlreadyExistsException();
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> cellValues)
            throws KeyAlreadyExistsException {
        queueOperation(
                CommandType.PUT_WITH_TIMESTAMPS,
                CommandTypeSerialisers.putWithTimestamps(tableRef, cellValues))
                .getResultWithKeyAlreadyExistsException();
    }

    @Override
    public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        queueOperation(
                CommandType.DELETE,
                CommandTypeSerialisers.delete(tableRef, keys))
                .getResultWithKeyAlreadyExistsException();
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return queueOperation(
                CommandType.READ_OPERATION,
                kvs -> kvs.get(tableRef, timestampByCell))
                .getResult();
    }

    @Override
    public Map<Cell, Value> getRows(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnSelection columnSelection,
            long timestamp) {
        return queueOperation(
                CommandType.READ_OPERATION,
                kvs -> kvs.getRows(tableRef, rows, columnSelection, timestamp))
                .getResult();
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(
            TableReference tableRef,
            RangeRequest rangeRequest,
            long timestamp) {
        return queueOperation(
                CommandType.READ_OPERATION,
                kvs -> kvs.getRange(tableRef, rangeRequest, timestamp))
                .getResult();
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection batchColumnRangeSelection,
            long timestamp) {
        return queueOperation(
                CommandType.READ_OPERATION,
                kvs -> kvs.getRowsColumnRange(tableRef, rows, batchColumnRangeSelection, timestamp))
                .getResult();
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection,
            int cellBatchHint,
            long timestamp) {
        return queueOperation(
                CommandType.READ_OPERATION,
                kvs -> kvs.getRowsColumnRange(tableRef, rows, columnRangeSelection, cellBatchHint, timestamp))
                .getResult();
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            long timestamp) {
        return queueOperation(
                CommandType.READ_OPERATION,
                kvs -> kvs.getFirstBatchForRanges(tableRef, rangeRequests, timestamp))
                .getResult();
    }

    @Override
    public Set<TableReference> getAllTableNames() {
        return getMetadataForTables().keySet();
    }

    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        return queueOperation(
                CommandType.READ_OPERATION,
                KeyValueService::getMetadataForTables)
                .getResult();
    }

    @Override
    public byte[] getMetadataForTable(TableReference tableRef) {
        return getMetadataForTables().get(tableRef);
    }

    @Override
    public void putMetadataForTable(TableReference tableRef, byte[] metadata) {
        putMetadataForTables(ImmutableMap.of(tableRef, metadata));
    }

    @Override
    public void putMetadataForTables(Map<TableReference, byte[]> tableRefToMetadata) {
        queueOperation(CommandType.PUT_METADATA_FOR_TABLES,
                CommandTypeSerialisers.putMetadataForTables(tableRefToMetadata))
                .getResult();
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        return queueOperation(
                CommandType.READ_OPERATION,
                kvs -> kvs.getLatestTimestamps(tableRef, timestampByCell))
                .getResult();
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(
            TableReference tableRef,
            RangeRequest rangeRequest,
            long timestamp)
            throws InsufficientConsistencyException {
        return queueOperation(
                CommandType.READ_OPERATION,
                kvs -> kvs.getRangeOfTimestamps(tableRef, rangeRequest, timestamp))
                .getResult();
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(
            TableReference tableRef,
            Set<Cell> cells,
            long timestamp)
            throws InsufficientConsistencyException {
        return queueOperation(
                CommandType.READ_OPERATION,
                kvs -> kvs.getAllTimestamps(tableRef, cells, timestamp))
                .getResult();
    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        queueOperation(
                CommandType.PUT_UNLESS_EXISTS,
                CommandTypeSerialisers.putUnlessExists(tableRef, values))
                .getResultWithKeyAlreadyExistsException();
    }

    private Operation<Void> queueOperation(CommandType commandType, byte[] data) {
        return queueOperation(commandType, data, kvs -> null);
    }

    private <R> Operation<R> queueOperation(CommandType commandType, Function<KeyValueService, R> functionToRun) {
        return queueOperation(commandType, null, functionToRun);
    }

    private <R> Operation<R> queueOperation(
            CommandType commandType,
            byte[] data,
            Function<KeyValueService, R> functionToRun) {
        Operation<R> operation = new Operation<>(Tag.generate());
        operationsToPerform.put(operation.getTag(), (kvs, ex) -> {
            if (ex.isPresent()) {
                operation.setResult(CompletableFuture.supplyAsync(() -> {
                    throw Throwables.propagate(ex.get());
                }));
            } else {
                operation.setResult(CompletableFuture.supplyAsync(() -> functionToRun.apply(kvs)));
            }
        });
        Futures.getUnchecked(producer.send(new ProducerRecord<>(
                OPERATIONS_TOPIC,
                commandType,
                ImmutableTaggedArray.builder()
                        .tag(operation.getTag())
                        .data(data)
                        .build())));
        return operation;
    }

    private static KafkaConsumer<CommandType, TaggedArray> createNewKafkaConsumer() {
        return new KafkaConsumer<>(ImmutableMap
                .<String, Object>builder()
                .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
                .put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString())
                .put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString())
                .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, CommandTypeDeserialiser.class.getCanonicalName())
                .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TaggedArrayDeserialiser.class.getCanonicalName())
                .build());
    }

    @Override
    public void addGarbageCollectionSentinelValues(TableReference tableRef, Set<Cell> cells) {
        throw new UnsupportedOperationException("Not supported on this KVS");
    }

    @Override
    public void compactInternally(TableReference tableRef) {
        throw new UnsupportedOperationException("Not supported on this KVS");
    }
}
