/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.schema.stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.ByteString;
import com.palantir.atlasdb.AtlasDbTestCase;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.protos.generated.StreamPersistence;
import com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata;
import com.palantir.atlasdb.schema.stream.generated.DeletingStreamStore;
import com.palantir.atlasdb.schema.stream.generated.KeyValueTable;
import com.palantir.atlasdb.schema.stream.generated.StreamTestMaxMemStreamStore;
import com.palantir.atlasdb.schema.stream.generated.StreamTestStreamHashAidxTable;
import com.palantir.atlasdb.schema.stream.generated.StreamTestStreamMetadataTable;
import com.palantir.atlasdb.schema.stream.generated.StreamTestStreamStore;
import com.palantir.atlasdb.schema.stream.generated.StreamTestStreamValueTable;
import com.palantir.atlasdb.schema.stream.generated.StreamTestTableFactory;
import com.palantir.atlasdb.schema.stream.generated.StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow;
import com.palantir.atlasdb.schema.stream.generated.StreamTestWithHashStreamMetadataTable;
import com.palantir.atlasdb.schema.stream.generated.StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow;
import com.palantir.atlasdb.schema.stream.generated.StreamTestWithHashStreamStore;
import com.palantir.atlasdb.schema.stream.generated.StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueRow;
import com.palantir.atlasdb.stream.PersistentStreamStore;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionConflictException;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.remoting2.tracing.Tracers;
import com.palantir.util.Pair;
import com.palantir.util.crypto.Sha256Hash;

public class StreamTest extends AtlasDbTestCase {
    private PersistentStreamStore defaultStore;
    private PersistentStreamStore compressedStore;
    private PersistentStreamStore maxMemStore;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void createSchema() {
        Schemas.deleteTablesAndIndexes(StreamTestSchema.getSchema(), keyValueService);
        Schemas.createTablesAndIndexes(StreamTestSchema.getSchema(), keyValueService);

        defaultStore = StreamTestStreamStore.of(txManager, StreamTestTableFactory.of());
        compressedStore = StreamTestWithHashStreamStore.of(txManager, StreamTestTableFactory.of());
        maxMemStore = StreamTestMaxMemStreamStore.of(txManager, StreamTestTableFactory.of());
    }

    @Test
    public void testRender() throws IOException {
        StreamTestSchema.getSchema().renderTables(temporaryFolder.getRoot());
    }

    @Test
    public void testAddDelete() throws Exception {
        final byte[] data = PtBytes.toBytes("streamed");
        final long streamId = txManager.runTaskWithRetry((TransactionTask<Long, Exception>) t -> {
            Sha256Hash hash = Sha256Hash.computeHash(data);
            byte[] reference = "ref".getBytes();

            return defaultStore.getByHashOrStoreStreamAndMarkAsUsed(t, hash, new ByteArrayInputStream(data), reference);
        });
        txManager.runTaskWithRetry((TransactionTask<Void, Exception>) t -> {
            Optional<InputStream> inputStream = defaultStore.loadSingleStream(t, streamId);
            assertTrue(inputStream.isPresent());
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            outputStream.write(inputStream.get());
            assertArrayEquals(data, outputStream.toByteArray());
            return null;
        });
    }

    @Test
    public void testLoadStreamWithWrongId() throws Exception {
        final byte[] data = PtBytes.toBytes("streamed");
        long streamId = txManager.runTaskWithRetry((TransactionTask<Long, Exception>) t -> {
            Sha256Hash hash = Sha256Hash.computeHash(data);
            byte[] reference = "ref".getBytes();

            return defaultStore.getByHashOrStoreStreamAndMarkAsUsed(t, hash,
                    new ByteArrayInputStream(data), reference);
        });
        txManager.runTaskWithRetry((TransactionTask<Void, Exception>) t -> {
            assertThat(defaultStore.loadSingleStream(t, streamId ^ 1L), is(Optional.empty()));
            return null;
        });
    }

    @Test
    public void testStreamStoreWithHashValueRowPersistToBytesAndHydrateSucceeds() {
        StreamTestWithHashStreamValueRow row = StreamTestWithHashStreamValueRow.of(5L, 5L);
        byte[] persistedRow = row.persistToBytes();
        StreamTestWithHashStreamValueRow hydratedRow =
                StreamTestWithHashStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(persistedRow);
        assertEquals(row, hydratedRow);
    }

    @Test
    public void testStreamStoreWithHashMetadataRowPersistToBytesAndHydrateSucceeds() {
        StreamTestWithHashStreamMetadataRow row = StreamTestWithHashStreamMetadataRow.of(5L);
        byte[] persistedRow = row.persistToBytes();
        StreamTestWithHashStreamMetadataRow hydratedRow =
                StreamTestWithHashStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(persistedRow);
        assertEquals(row, hydratedRow);
    }

    @Test
    public void testStreamStoreWithHashIdxRowPersistToBytesAndHydrateSucceeds() {
        StreamTestWithHashStreamIdxRow row = StreamTestWithHashStreamIdxRow.of(5L);
        byte[] persistedRow = row.persistToBytes();
        StreamTestWithHashStreamIdxRow hydratedRow =
                StreamTestWithHashStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(persistedRow);
        assertEquals(row, hydratedRow);
    }

    @Test
    public void testStoreEmptyByteStream_defaultStream() throws IOException {
        storeAndCheckByteStreams(defaultStore, getIncompressibleBytes(0));
    }

    @Test
    public void testStoreEmptyByteStream_compressedStream() throws IOException {
        storeAndCheckByteStreams(compressedStore, getIncompressibleBytes(0));
    }

    @Test
    public void testStoreSmallByteStream_defaultStream() throws IOException {
        storeAndCheckByteStreams(defaultStore, getIncompressibleBytes(100));
    }

    @Test
    public void testStoreByteStreamExactlyAtInMemoryThreshold() throws IOException {
        storeAndCheckByteStreams(defaultStore, getIncompressibleBytes(StreamTestStreamStore.BLOCK_SIZE_IN_BYTES * 4));
    }

    @Test
    public void testStoreByteStreamJustAboveInMemoryThreshold() throws IOException {
        storeAndCheckByteStreams(defaultStore,
                getIncompressibleBytes(StreamTestStreamStore.BLOCK_SIZE_IN_BYTES * 4 + 1));
    }

    @Test
    public void testStoreSmallByteStream_compressedStream() throws IOException {
        storeAndCheckByteStreams(compressedStore, getCompressibleBytes(100));
    }

    @Test
    public void testStoreByteStreamJustBiggerThanOneBlock_defaultStream() throws IOException {
        storeAndCheckByteStreams(defaultStore,
                getIncompressibleBytes(StreamTestStreamStore.BLOCK_SIZE_IN_BYTES + 500));
    }

    @Test
    public void testStoreByteStreamJustBiggerThanOneBlock_compressedStream() throws IOException {
        storeAndCheckByteStreams(compressedStore,
                getCompressibleBytes(StreamTestStreamStore.BLOCK_SIZE_IN_BYTES + 500));
    }

    @Test
    public void testStoreByteStreamThreeBlocksLong_defaultStream() throws IOException {
        storeAndCheckByteStreams(defaultStore, getIncompressibleBytes(StreamTestStreamStore.BLOCK_SIZE_IN_BYTES * 3));
    }

    @Test
    public void testStoreByteStreamThreeBlocksLong_compressedStream() throws IOException {
        storeAndCheckByteStreams(compressedStore, getCompressibleBytes(StreamTestStreamStore.BLOCK_SIZE_IN_BYTES * 3));
    }

    @Test
    public void testStoreByteStreamFiveMegaBytes_defaultStream() throws IOException {
        storeAndCheckByteStreams(defaultStore, getIncompressibleBytes(5_000_000));
    }

    @Test
    public void testStoreByteStreamFiveMegaBytes_compressedStream_compressible() throws IOException {
        storeAndCheckByteStreams(compressedStore, getCompressibleBytes(5_000_000));
    }

    @Test
    public void testStoreByteStreamFiveMegaBytes_compressedStream_incompressible() throws IOException {
        storeAndCheckByteStreams(compressedStore, getIncompressibleBytes(5_000_000));
    }

    @Test
    public void testStoreToMaxMemStream() throws IOException {
        storeAndCheckByteStreams(maxMemStore, getIncompressibleBytes(100));
    }

    @Test
    public void testStoreHugeToMaxMemStream() throws IOException {
        storeAndCheckByteStreams(maxMemStore, getIncompressibleBytes(20_000_000));
    }

    private long storeAndCheckByteStreams(PersistentStreamStore store, byte[] bytesToStore) throws IOException {
        byte[] reference = PtBytes.toBytes("ref");

        final long id = storeStream(store, bytesToStore, reference);

        verifyLoadingStreams(store, id, bytesToStore);

        store.storeStream(new ByteArrayInputStream(bytesToStore));
        verifyLoadingStreams(store, id, bytesToStore);

        return id;
    }

    private long storeStream(PersistentStreamStore store, byte[] bytesToStore, byte[] reference) {
        final long id = timestampService.getFreshTimestamp();
        txManager.runTaskWithRetry(t -> {
            store.storeStreams(t, ImmutableMap.of(id, new ByteArrayInputStream(bytesToStore)));
            store.markStreamAsUsed(t, id, reference);
            return null;
        });

        return id;
    }

    private void verifyLoadingStreams(PersistentStreamStore store, long id, byte[] bytesToStore) throws IOException {
        verifyLoadStream(store, id, bytesToStore);
        verifyLoadSingleStream(store, id, bytesToStore);
        verifyLoadStreams(store, id, bytesToStore);
        verifyLoadStreamAsFile(store, id, bytesToStore);
    }

    @SuppressWarnings("deprecation")
    private void verifyLoadStream(PersistentStreamStore store, long id, byte[] bytesToStore) throws IOException {
        InputStream stream = txManager.runTaskThrowOnConflict(t -> store.loadStream(t, id));
        assertStreamHasBytes(stream, bytesToStore);
    }

    private void verifyLoadSingleStream(PersistentStreamStore store, long id, byte[] toStore) throws IOException {
        Optional<InputStream> stream = txManager.runTaskThrowOnConflict(t -> store.loadSingleStream(t, id));
        assertTrue(stream.isPresent());
        assertStreamHasBytes(stream.get(), toStore);
    }

    private void verifyLoadStreams(PersistentStreamStore store, long id, byte[] bytesToStore) throws IOException {
        Map<Long, InputStream> streams = txManager.runTaskThrowOnConflict(t ->
                store.loadStreams(t, ImmutableSet.of(id)));
        assertStreamHasBytes(streams.get(id), bytesToStore);
    }

    private void verifyLoadStreamAsFile(PersistentStreamStore store, long id, byte[] bytesToStore) throws IOException {
        File file = txManager.runTaskThrowOnConflict(t -> store.loadStreamAsFile(t, id));
        assertArrayEquals(bytesToStore, FileUtils.readFileToByteArray(file));
    }

    private void assertStreamHasBytes(InputStream stream, byte[] bytes) throws IOException {
        byte[] streamAsBytes = IOUtils.toByteArray(stream);
        assertArrayEquals(bytes, streamAsBytes);
        stream.close();
    }

    @Test
    public void readFromStreamWhenTransactionOpen() throws IOException {
        readFromGivenStreamWhenTransactionOpen(defaultStore);
    }

    @Test
    public void readFromCompressedStreamWhenTransactionOpen() throws IOException {
        readFromGivenStreamWhenTransactionOpen(compressedStore);
    }

    private void readFromGivenStreamWhenTransactionOpen(PersistentStreamStore store) throws IOException {
        byte[] reference = PtBytes.toBytes("ref");
        byte[] data = getIncompressibleBytes(StreamTestStreamStore.BLOCK_SIZE_IN_BYTES * 3);

        final long id = storeStream(store,
                data,
                reference);

        txManager.runTaskThrowOnConflict(t -> {
            // use the stream (read from it) inside the same transaction
            try (InputStream stream = store.loadStream(t, id)) {
                assertStreamHasBytes(stream, data);
            }
            return null;
        });
    }

    @Test
    public void testOverwrite() throws IOException {
        Random rand = new Random();
        StreamTestTableFactory tableFactory = StreamTestTableFactory.of();

        final byte[] reference = PtBytes.toBytes("ref");
        final byte[] bytes1 = new byte[2 * StreamTestStreamStore.BLOCK_SIZE_IN_BYTES];
        rand.nextBytes(bytes1);

        KeyValueTable.KeyValueRow keyValueRow = KeyValueTable.KeyValueRow.of("ref");

        // Store the stream, together with a reference
        Long streamId = txManager.runTaskWithRetry(tx -> {
            long id = storeStream(defaultStore, bytes1, reference);
            KeyValueTable keyValueTable = tableFactory.getKeyValueTable(tx);
            keyValueTable.putStreamId(keyValueRow, id);
            return id;
        });

        // Then fetch streamId as an input stream
        InputStream firstStream = txManager.runTaskWithRetry(tx -> defaultStore.loadStream(tx, streamId));

        // Then store "ref" -> some_other_stream
        final byte[] bytes2 = new byte[2 * StreamTestStreamStore.BLOCK_SIZE_IN_BYTES];
        rand.nextBytes(bytes2);
        storeStreamAndReference(tableFactory, keyValueRow, reference, bytes2);

        // Then continue to read - it should be OK
        assertStreamHasBytes(firstStream, bytes1);
    }

    private void storeStreamAndReference(StreamTestTableFactory tableFactory, KeyValueTable.KeyValueRow row,
            byte[] reference, byte[] value) {
        txManager.runTaskWithRetry(tx -> {
            long id = storeStream(defaultStore, value, reference);
            KeyValueTable keyValueTable = tableFactory.getKeyValueTable(tx);
            keyValueTable.putStreamId(row, id);
            return null;
        });
    }

    @Test(expected = NullPointerException.class)
    public void testConcurrentDelete() throws IOException {
        Random rand = new Random();
        StreamTestTableFactory tableFactory = StreamTestTableFactory.of();

        final byte[] reference = PtBytes.toBytes("ref");
        final byte[] bytes1 = new byte[2 * StreamTestStreamStore.IN_MEMORY_THRESHOLD];
        rand.nextBytes(bytes1);

        KeyValueTable.KeyValueRow keyValueRow = KeyValueTable.KeyValueRow.of("ref");

        // Store the stream, together with a reference
        Long streamId = txManager.runTaskWithRetry(tx -> {
            long id = storeStream(defaultStore, bytes1, reference);
            KeyValueTable keyValueTable = tableFactory.getKeyValueTable(tx);
            keyValueTable.putStreamId(keyValueRow, id);
            return id;
        });

        // Then fetch streamId as an input stream
        InputStream stream = txManager.runTaskWithRetry(tx -> defaultStore.loadStream(tx, streamId));

        // Delete the streams
        txManager.runTaskWithRetry(tx -> {
            KeyValueTable keyValueTable = tableFactory.getKeyValueTable(tx);
            keyValueTable.delete(keyValueRow);
            deleteStream(tableFactory, tx, streamId);
            return null;
        });

        // Gives a null pointer exception.
        assertStreamHasBytes(stream, bytes1);
    }

    private void deleteStream(StreamTestTableFactory tableFactory, Transaction tx, Long streamId) {
        StreamPersistence.StreamMetadata metadata = getMetadata(tableFactory, tx, streamId);
        deleteStreamHashEntry(tableFactory, tx, streamId, metadata.getHash());
        deleteStreamValues(tableFactory, tx, streamId, getNumberOfBlocks(metadata));
    }

    private StreamPersistence.StreamMetadata getMetadata(StreamTestTableFactory tableFactory, Transaction tx,
            Long streamId) {
        StreamTestStreamMetadataTable table = tableFactory.getStreamTestStreamMetadataTable(tx);

        Set<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow> smRows = Sets.newHashSet();
        smRows.add(StreamTestStreamMetadataTable.StreamTestStreamMetadataRow.of(streamId));

        Map<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow,
                StreamPersistence.StreamMetadata> metadatas = table.getMetadatas(smRows);
        return Iterables.getOnlyElement(metadatas.values());
    }

    private void deleteStreamHashEntry(StreamTestTableFactory tableFactory, Transaction tx, Long streamId,
            ByteString streamHash) {
        Sha256Hash hash = new Sha256Hash(streamHash.toByteArray());
        StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow hashRow =
                StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow.of(hash);
        StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumn column =
                StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumn.of(streamId);

        Multimap<StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow,
                StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumn> shToDelete =
                ImmutableMultimap.of(hashRow, column);

        tableFactory.getStreamTestStreamHashAidxTable(tx).delete(shToDelete);
    }

    private int getNumberOfBlocks(StreamPersistence.StreamMetadata metadata) {
        return (int) ((metadata.getLength() + StreamTestStreamStore.BLOCK_SIZE_IN_BYTES - 1)
                / StreamTestStreamStore.BLOCK_SIZE_IN_BYTES);
    }

    private void deleteStreamValues(StreamTestTableFactory tableFactory, Transaction tx, Long streamId, int numBlocks) {
        Set<StreamTestStreamValueTable.StreamTestStreamValueRow> streamValueToDelete = Sets.newHashSet();
        for (long i = 0; i < numBlocks; i++) {
            streamValueToDelete.add(StreamTestStreamValueTable.StreamTestStreamValueRow.of(streamId, i));
        }

        tableFactory.getStreamTestStreamValueTable(tx).delete(streamValueToDelete);
    }

    @Test
    public void testLookupStreamIdsByHash() throws Exception {
        final byte[] bytes1 = new byte[2 * StreamTestStreamStore.BLOCK_SIZE_IN_BYTES];
        final byte[] bytes2 = new byte[2 * StreamTestStreamStore.BLOCK_SIZE_IN_BYTES];

        long id1 = timestampService.getFreshTimestamp();
        long id2 = timestampService.getFreshTimestamp();

        Random rand = new Random();
        rand.nextBytes(bytes1);
        rand.nextBytes(bytes2);
        Sha256Hash hash1 = Sha256Hash.computeHash(bytes1);
        Sha256Hash hash2 = Sha256Hash.computeHash(bytes2);
        Sha256Hash hash3 = Sha256Hash.EMPTY;

        ImmutableMap<Long, InputStream> streams = ImmutableMap.of(
                id1, new ByteArrayInputStream(bytes1),
                id2, new ByteArrayInputStream(bytes2));

        txManager.runTaskWithRetry(t -> defaultStore.storeStreams(t, streams));

        Map<Sha256Hash, Long> sha256HashLongMap = txManager.runTaskWithRetry(
                t -> defaultStore.lookupStreamIdsByHash(t, ImmutableSet.of(hash1, hash2, hash3)));

        assertEquals(id1, sha256HashLongMap.get(hash1).longValue());
        assertEquals(id2, sha256HashLongMap.get(hash2).longValue());
        assertNull(sha256HashLongMap.get(hash3));
    }

    @Test
    public void testStoreCopy() {
        final byte[] bytes = new byte[2 * StreamTestStreamStore.BLOCK_SIZE_IN_BYTES];
        Random rand = new Random();
        rand.nextBytes(bytes);

        long id1 = timestampService.getFreshTimestamp();
        long id2 = timestampService.getFreshTimestamp();

        ImmutableMap<Long, InputStream> streams = ImmutableMap.of(
                id1, new ByteArrayInputStream(bytes),
                id2, new ByteArrayInputStream(bytes));

        txManager.runTaskWithRetry(t -> defaultStore.storeStreams(t, streams));

        Pair<Long, Sha256Hash> idAndHash1 = defaultStore.storeStream(new ByteArrayInputStream(bytes));
        Pair<Long, Sha256Hash> idAndHash2 = defaultStore.storeStream(new ByteArrayInputStream(bytes));

        assertThat(idAndHash1.getRhSide(), equalTo(idAndHash2.getRhSide()));        //verify hashes are the same
        assertThat(idAndHash1.getLhSide(), not(equalTo(idAndHash2.getLhSide())));   //verify ids are different
    }

    @Test
    public void testStreamMetadataConflictDeleteFirst() throws Exception {
        long streamId = timestampService.getFreshTimestamp();

        runConflictingTasksConcurrently(streamId, new TwoConflictingTasks() {
            @Override
            public void startFirstAndFail(Transaction tx, long streamId) {
                StreamTestStreamStore ss = StreamTestStreamStore.of(txManager, StreamTestTableFactory.of());
                ss.storeStreams(tx, ImmutableMap.of(streamId, new ByteArrayInputStream(new byte[1])));
            }

            @Override
            public void startSecondAndFinish(Transaction tx, long streamId) {
                DeletingStreamStore deletingStreamStore = new DeletingStreamStore(
                        StreamTestStreamStore.of(txManager, StreamTestTableFactory.of()));
                deletingStreamStore.deleteStreams(tx, ImmutableSet.of(streamId));
            }
        });

        assertStreamDoesNotExist(streamId);
    }

    @Test
    public void testStreamMetadataConflictWriteFirst() throws Exception {
        long streamId = timestampService.getFreshTimestamp();

        runConflictingTasksConcurrently(streamId, new TwoConflictingTasks() {
            @Override
            public void startFirstAndFail(Transaction tx, long streamId) {
                DeletingStreamStore deletingStreamStore = new DeletingStreamStore(
                        StreamTestStreamStore.of(txManager, StreamTestTableFactory.of()));
                deletingStreamStore.deleteStreams(tx, ImmutableSet.of(streamId));
            }

            @Override
            public void startSecondAndFinish(Transaction tx, long streamId) {
                StreamTestStreamStore ss = StreamTestStreamStore.of(txManager, StreamTestTableFactory.of());
                ss.storeStreams(tx, ImmutableMap.of(streamId, new ByteArrayInputStream(new byte[1])));
            }
        });

        Optional<InputStream> stream = getStream(streamId);
        assertTrue(stream.isPresent());
        assertNotNull(stream.get());
    }

    @Test
    public void testStreamCompression() throws IOException {
        int inputBlocks = 4;
        int expectedBlocksUsed = 1;
        byte[] input = getCompressibleBytes(inputBlocks * StreamTestWithHashStreamStore.BLOCK_SIZE_IN_BYTES);

        long id = storeStream(compressedStore, input, PtBytes.toBytes("ref"));
        long numBlocksUsed = getStreamBlockSize(getStreamMetadata(id));

        assertEquals(expectedBlocksUsed, numBlocksUsed);
    }

    private StreamMetadata getStreamMetadata(long id) {
        return txManager.runTaskReadOnly(t -> {
            StreamTestWithHashStreamMetadataTable table = StreamTestTableFactory.of()
                    .getStreamTestWithHashStreamMetadataTable(t);
            StreamTestWithHashStreamMetadataRow row = StreamTestWithHashStreamMetadataRow.of(id);
            return table.getRow(row).get().getMetadata();
        });
    }

    private long getStreamBlockSize(StreamMetadata metadata) {
        int blockSize = StreamTestWithHashStreamStore.BLOCK_SIZE_IN_BYTES;
        return (metadata.getLength() + blockSize - 1) / blockSize;
    }

    private Optional<InputStream> getStream(long streamId) {
        return txManager.runTaskThrowOnConflict(t -> {
            StreamTestStreamStore streamStore = StreamTestStreamStore.of(txManager, StreamTestTableFactory.of());
            return streamStore.loadSingleStream(t, streamId);
        });
    }

    private void assertStreamDoesNotExist(final long streamId) {
        assertFalse("This element should have been deleted", getStream(streamId).isPresent());
    }

    private void runConflictingTasksConcurrently(long streamId, TwoConflictingTasks tasks) throws InterruptedException {
        final CountDownLatch firstLatch = new CountDownLatch(1);
        final CountDownLatch secondLatch = new CountDownLatch(1);

        ExecutorService exec = Tracers.wrap(PTExecutors.newFixedThreadPool(2));

        Future<?> firstFuture = exec.submit(() -> {
            try {
                txManager.runTaskThrowOnConflict(t -> {
                    tasks.startFirstAndFail(t, streamId);
                    letOtherTaskFinish(firstLatch, secondLatch);
                    return null;
                });
                fail("Because we concurrently wrote, we should have failed with TransactionConflictException.");
            } catch (TransactionConflictException e) {
                // expected
            }
        });

        firstLatch.await();

        Future<?> secondFuture = exec.submit((Runnable) () ->
                txManager.runTaskThrowOnConflict((TransactionTask<Void, RuntimeException>) t -> {
                    tasks.startSecondAndFinish(t, streamId);
                    return null;
                }));

        exec.shutdown();
        Futures.getUnchecked(secondFuture);

        secondLatch.countDown();
        Futures.getUnchecked(firstFuture);
    }

    private void letOtherTaskFinish(CountDownLatch firstLatch, CountDownLatch secondLatch) {
        firstLatch.countDown();
        try {
            secondLatch.await();
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

    private abstract class TwoConflictingTasks {
        public abstract void startFirstAndFail(Transaction tx, long streamId);
        public abstract void startSecondAndFinish(Transaction tx, long streamId);
    }

    private byte[] getCompressibleBytes(int size) {
        byte[] data = new byte[size];
        Arrays.fill(data, (byte) 42);
        return data;
    }

    private byte[] getIncompressibleBytes(int size) {
        byte[] data = new byte[size];
        new Random(0).nextBytes(data);
        return data;
    }
}
