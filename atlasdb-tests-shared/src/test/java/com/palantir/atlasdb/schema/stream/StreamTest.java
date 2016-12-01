/**
 * Copyright 2015 Palantir Technologies
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.AtlasDbTestCase;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.schema.stream.generated.DeletingStreamStore;
import com.palantir.atlasdb.schema.stream.generated.KeyValueTable;
import com.palantir.atlasdb.schema.stream.generated.StreamTestIndexCleanupTask;
import com.palantir.atlasdb.schema.stream.generated.StreamTestStreamStore;
import com.palantir.atlasdb.schema.stream.generated.StreamTestTableFactory;
import com.palantir.atlasdb.schema.stream.generated.StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow;
import com.palantir.atlasdb.schema.stream.generated.StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow;
import com.palantir.atlasdb.schema.stream.generated.StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueRow;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionConflictException;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.util.Pair;
import com.palantir.util.crypto.Sha256Hash;

public class StreamTest extends AtlasDbTestCase {
    private StreamTestStreamStore store;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void createSchema() {
        Schemas.deleteTablesAndIndexes(StreamTestSchema.getSchema(), keyValueService);
        Schemas.createTablesAndIndexes(StreamTestSchema.getSchema(), keyValueService);

        store = StreamTestStreamStore.of(txManager, StreamTestTableFactory.of());
    }

    @Test
    public void testRender() throws IOException {
        StreamTestSchema.getSchema().renderTables(temporaryFolder.getRoot());
    }

    @Test
    public void testAddDelete() throws Exception {
        final byte[] data = PtBytes.toBytes("streamed");
        final long streamId = txManager.runTaskWithRetry(new TransactionTask<Long, Exception>() {
            @Override
            public Long execute(Transaction t) throws Exception {
                byte[] data = PtBytes.toBytes("streamed");
                Sha256Hash hash = Sha256Hash.computeHash(data);
                byte[] reference = "ref".getBytes();
                long streamId = store.getByHashOrStoreStreamAndMarkAsUsed(t, hash, new ByteArrayInputStream(data), reference);
                try {
                    store.loadStream(t, 1L).read(data, 0, data.length);
                } catch (NoSuchElementException e) {
                    // expected
                }
                return streamId;
            }
        });
        txManager.runTaskWithRetry(new TransactionTask<Void, Exception>() {
            @Override
            public Void execute(Transaction t) throws Exception {
                Assert.assertEquals(data.length, store.loadStream(t, streamId).read(data, 0, data.length));
                return null;
            }
        });
    }

    @Test
    public void testStreamStoreWithHashValueRowPersistToBytesAndHydrateSucceeds() {
        StreamTestWithHashStreamValueRow row = StreamTestWithHashStreamValueRow.of(5L, 5L);
        byte[] persistedRow = row.persistToBytes();
        StreamTestWithHashStreamValueRow hydratedRow =
                StreamTestWithHashStreamValueRow.BYTES_HYDRATOR.hydrateFromBytes(persistedRow);
        Assert.assertEquals(row, hydratedRow);
    }

    @Test
    public void testStreamStoreWithHashMetadataRowPersistToBytesAndHydrateSucceeds() {
        StreamTestWithHashStreamMetadataRow row = StreamTestWithHashStreamMetadataRow.of(5L);
        byte[] persistedRow = row.persistToBytes();
        StreamTestWithHashStreamMetadataRow hydratedRow =
                StreamTestWithHashStreamMetadataRow.BYTES_HYDRATOR.hydrateFromBytes(persistedRow);
        Assert.assertEquals(row, hydratedRow);
    }

    @Test
    public void testStreamStoreWithHashIdxRowPersistToBytesAndHydrateSucceeds() {
        StreamTestWithHashStreamIdxRow row = StreamTestWithHashStreamIdxRow.of(5L);
        byte[] persistedRow = row.persistToBytes();
        StreamTestWithHashStreamIdxRow hydratedRow =
                StreamTestWithHashStreamIdxRow.BYTES_HYDRATOR.hydrateFromBytes(persistedRow);
        Assert.assertEquals(row, hydratedRow);
    }

    @Test
    public void testStoreEmptyByteStream() throws IOException {
        storeAndCheckByteStreams(0);
    }

    @Test
    public void testStoreSmallByteStream() throws IOException {
        storeAndCheckByteStreams(100);
    }

    @Test
    public void testStoreByteStreamJustBiggerThanOneBlock() throws IOException {
        storeAndCheckByteStreams(StreamTestStreamStore.BLOCK_SIZE_IN_BYTES + 500);
    }

    @Test
    public void testStoreByteStreamThreeBlocksLong() throws IOException {
        storeAndCheckByteStreams(StreamTestStreamStore.BLOCK_SIZE_IN_BYTES * 3);
    }

    @Test
    public void testStoreByteStreamFiveMegaBytes() throws IOException {
        storeAndCheckByteStreams(5_000_000);
    }

    private long storeAndCheckByteStreams(int size) throws IOException {
        byte[] reference = PtBytes.toBytes("ref");
        final byte[] bytesToStore = new byte[size];
        Random rand = new Random();
        rand.nextBytes(bytesToStore);

        final long id = storeStream(bytesToStore, reference);

        verifyLoadingStreams(id, bytesToStore);

        store.storeStream(new ByteArrayInputStream(bytesToStore));
        verifyLoadingStreams(id, bytesToStore);

        return id;
    }

    @Test
    public void shouldQuicklyGetFirstByte() throws IOException {
        long millisFor100mb = measureMillisToGetFirstByteFromStreamStoreWithMbs(100);
        long millisFor1mb = measureMillisToGetFirstByteFromStreamStoreWithMbs(1);

        assertTrue("Loading the first byte of a stream should perform better than log(size). "
                + "We actually expect constant time, but are being permissive. "
                + "Fetching 1MB took " + millisFor1mb + "ms, but 100 MB took " + millisFor100mb + "ms.",
                millisFor100mb < (millisFor1mb * 10));
    }

    private long measureMillisToGetFirstByteFromStreamStoreWithMbs(int megabytes) throws IOException {
        final int size = megabytes * 1024 * 1024;
        byte[] bytesToStore = new byte[size];
        Random rand = new Random();
        rand.nextBytes(bytesToStore);

        final long id = storeStream(bytesToStore, PtBytes.toBytes(megabytes));

        Stopwatch timer = Stopwatch.createStarted();
        InputStream stream = txManager.runTaskThrowOnConflict(t -> store.loadStream(t, id));
        byte[] sample = new byte[1];
        //noinspection ResultOfMethodCallIgnored
        stream.read(sample);
        timer.stop();
        Assert.assertEquals(bytesToStore[0], sample[0]);
        long millis = timer.elapsed(TimeUnit.MILLISECONDS);
        System.out.println(String.format("Getting first byte of %dMB took %d milliseconds.", megabytes, millis));
        return millis;
    }

    private long storeStream(byte[] bytesToStore, byte[] reference) {
        final long id = timestampService.getFreshTimestamp();
        txManager.runTaskWithRetry(t -> {
            store.storeStreams(t, ImmutableMap.of(id, new ByteArrayInputStream(bytesToStore)));
            store.markStreamAsUsed(t, id, reference);
            return null;
        });

        return id;
    }

    private void verifyLoadingStreams(long id, byte[] bytesToStore) throws IOException {
        verifyLoadStream(id, bytesToStore);
        verifyLoadStreams(id, bytesToStore);
        verifyLoadStreamAsFile(id, bytesToStore);
    }

    private void verifyLoadStreamAsFile(long id, byte[] bytesToStore) throws IOException {
        File file = txManager.runTaskThrowOnConflict(t -> store.loadStreamAsFile(t, id));
        Assert.assertArrayEquals(bytesToStore, FileUtils.readFileToByteArray(file));
    }

    private void verifyLoadStreams(long id, byte[] bytesToStore) throws IOException {
        Map<Long, InputStream> streams = txManager.runTaskThrowOnConflict(t ->
                store.loadStreams(t, ImmutableSet.of(id)));
        assertStreamHasBytes(streams.get(id), bytesToStore);
    }

    private void verifyLoadStream(long id, byte[] bytesToStore) throws IOException {
        InputStream stream = txManager.runTaskThrowOnConflict(t -> store.loadStream(t, id));
        assertStreamHasBytes(stream, bytesToStore);
    }

    private void assertStreamHasBytes(InputStream stream, byte[] bytes) throws IOException {
        byte[] streamAsBytes = IOUtils.toByteArray(stream);
        Assert.assertArrayEquals(bytes, streamAsBytes);
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
            long id = storeStream(bytes1, reference);
            KeyValueTable keyValueTable = tableFactory.getKeyValueTable(tx);
            keyValueTable.putStreamId(keyValueRow, id);
            return id;
        });

        // Then fetch streamId as an input stream
        InputStream firstStream = txManager.runTaskWithRetry(tx -> store.loadStream(tx, streamId));

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
            long id = storeStream(value, reference);
            KeyValueTable keyValueTable = tableFactory.getKeyValueTable(tx);
            keyValueTable.putStreamId(row, id);
            return null;
        });
    }

    @Test
    public void testConcurrentOverwrite() throws IOException {
        Random rand = new Random();
        StreamTestTableFactory tableFactory = StreamTestTableFactory.of();

        final byte[] reference = PtBytes.toBytes("ref");
        final byte[] bytes1 = new byte[2 * StreamTestStreamStore.BLOCK_SIZE_IN_BYTES];
        rand.nextBytes(bytes1);

        KeyValueTable.KeyValueRow row = KeyValueTable.KeyValueRow.of("ref");

        // Store the stream, together with a reference
        txManager.runTaskWithRetry(tx -> {
            long id = storeStream(bytes1, reference);
            KeyValueTable keyValueTable = tableFactory.getKeyValueTable(tx);
            keyValueTable.putStreamId(row, id);
            return null;
        });

        // Then fetch streamId as an input stream
        final byte[] bytes2 = new byte[2 * StreamTestStreamStore.BLOCK_SIZE_IN_BYTES];
        rand.nextBytes(bytes2);
        SerializableTransactionManager manager = new SerializableTransactionManager(
                keyValueService, timestampService, lockClient, lockService, transactionService,
                Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                conflictDetectionManager, sweepStrategyManager, NoOpCleaner.INSTANCE, false);

        // TODO This should cause a read-write conflict. At time t1, we start this transaction, and do a read
        // TODO of (ref, STREAM_ID), and get the streamId from above (1, let's say).
        // TODO However, before committing, we do a write of (ref, STREAM_ID), putting 2 (let's say).
        // TODO Only then do we commit the read transaction - and at this point we should realise that the value read
        // TODO is now out of date.
        byte[] result = manager.runTaskReadOnly(tx -> {
            KeyValueTable keyValueTable = tableFactory.getKeyValueTable(tx);
            KeyValueTable.KeyValueNamedColumn column = KeyValueTable.KeyValueNamedColumn.STREAM_ID;
            ColumnSelection columns = KeyValueTable.getColumnSelection(column);
            List<KeyValueTable.KeyValueNamedColumnValue<?>> rowColumns = keyValueTable.getRowColumns(row, columns);
            Long streamId = (Long) Iterables.getOnlyElement(rowColumns).getValue();

            InputStream stream = store.loadStream(tx, streamId);

            manager.runTaskWithRetry(tx1 -> {
                long id = storeStream(bytes2, reference);
                KeyValueTable keyValueTable1 = tableFactory.getKeyValueTable(tx1);
                keyValueTable1.putStreamId(row, id);
                return null;
            });

            return IOUtils.toByteArray(stream);
        });

        assertArrayEquals(bytes1, result);
    }

    @Test
    public void testConcurrentDelete() throws IOException {
        Random rand = new Random();
        StreamTestTableFactory tableFactory = StreamTestTableFactory.of();

        final byte[] reference = PtBytes.toBytes("ref");
        final byte[] bytes1 = new byte[2 * StreamTestStreamStore.BLOCK_SIZE_IN_BYTES];
        rand.nextBytes(bytes1);

        KeyValueTable.KeyValueRow keyValueRow = KeyValueTable.KeyValueRow.of("ref");

        // Store the stream, together with a reference
        Long streamId = txManager.runTaskWithRetry(tx -> {
            long id = storeStream(bytes1, reference);
            KeyValueTable keyValueTable = tableFactory.getKeyValueTable(tx);
            keyValueTable.putStreamId(keyValueRow, id);
            return id;
        });

        // Then fetch streamId as an input stream
        InputStream firstStream = txManager.runTaskWithRetry(tx -> store.loadStream(tx, streamId));

        // Delete the streams
        txManager.runTaskWithRetry(tx -> {
            KeyValueTable keyValueTable = tableFactory.getKeyValueTable(tx);
            keyValueTable.delete(keyValueRow);
            return null;
        });

        StreamTestIndexCleanupTask cleanup = new StreamTestIndexCleanupTask(Namespace.DEFAULT_NAMESPACE);
        byte[] colBytes = KeyValueTable.KeyValueNamedColumn.STREAM_ID.name().getBytes();
        Cell cell = Cell.create(keyValueRow.getKey().getBytes(), colBytes);

        txManager.runTaskWithRetry(tx -> {
            store.deleteStreams(tx, ImmutableSet.of(streamId));
            return null;
        });
            //return cleanup.cellsCleanedUp(tx, ImmutableSet.of(cell));

        // Gives a null pointer exception.
        assertStreamHasBytes(firstStream, bytes1);
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

        txManager.runTaskWithRetry(t -> store.storeStreams(t, streams));

        Map<Sha256Hash, Long> sha256HashLongMap = txManager.runTaskWithRetry(t -> store.lookupStreamIdsByHash(t, ImmutableSet.of(hash1, hash2, hash3)));

        assertEquals(id1, sha256HashLongMap.get(hash1).longValue());
        assertEquals(id2, sha256HashLongMap.get(hash2).longValue());
        assertEquals(null, sha256HashLongMap.get(hash3));
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

        txManager.runTaskWithRetry(t -> store.storeStreams(t, streams));

        Pair<Long, Sha256Hash> idAndHash1 = store.storeStream(new ByteArrayInputStream(bytes));
        Pair<Long, Sha256Hash> idAndHash2 = store.storeStream(new ByteArrayInputStream(bytes));

        assertThat(idAndHash1.getRhSide(), equalTo(idAndHash2.getRhSide()));        //verify hashes are the same
        assertThat(idAndHash1.getLhSide(), not(equalTo(idAndHash2.getLhSide())));   //verify ids are different
    }

    @Test
    public void testStreamMetadataConflictDeleteFirst() throws Exception {
        long streamId = timestampService.getFreshTimestamp();

        runConflictingTasksConcurrently(streamId, new TwoConflictingTasks() {
            @Override
            public void startFirstAndFail(Transaction t, long streamId) {
                StreamTestStreamStore ss = StreamTestStreamStore.of(txManager, StreamTestTableFactory.of());
                ss.storeStreams(t, ImmutableMap.of(streamId, new ByteArrayInputStream(new byte[1])));
            }

            @Override
            public void startSecondAndFinish(Transaction t, long streamId) {
                DeletingStreamStore deletingStreamStore = new DeletingStreamStore(StreamTestStreamStore.of(txManager, StreamTestTableFactory.of()));
                deletingStreamStore.deleteStreams(t, ImmutableSet.of(streamId));
            }
        });

        assertStreamDoesNotExist(streamId);
    }

    @Test
    public void testStreamMetadataConflictWriteFirst() throws Exception {
        long streamId = timestampService.getFreshTimestamp();

        runConflictingTasksConcurrently(streamId, new TwoConflictingTasks() {
            @Override
            public void startFirstAndFail(Transaction t, long streamId) {
                DeletingStreamStore deletingStreamStore = new DeletingStreamStore(StreamTestStreamStore.of(txManager, StreamTestTableFactory.of()));
                deletingStreamStore.deleteStreams(t, ImmutableSet.of(streamId));
            }

            @Override
            public void startSecondAndFinish(Transaction t, long streamId) {
                StreamTestStreamStore ss = StreamTestStreamStore.of(txManager, StreamTestTableFactory.of());
                ss.storeStreams(t, ImmutableMap.of(streamId, new ByteArrayInputStream(new byte[1])));
            }
        });

        assertNotNull(getStream(streamId));
    }

    private InputStream getStream(long streamId) {
        return txManager.runTaskThrowOnConflict(t -> {
            StreamTestStreamStore streamStore = StreamTestStreamStore.of(txManager, StreamTestTableFactory.of());
            return streamStore.loadStream(t, streamId);
        });
    }

    private void assertStreamDoesNotExist(final long streamId) {
        try {
            getStream(streamId);
            fail("This element should have been deleted");
        } catch (NoSuchElementException e) {
            // expected
        }
    }

    private void runConflictingTasksConcurrently(long streamId, TwoConflictingTasks twoConflictingTasks) throws InterruptedException {
        final CountDownLatch firstLatch = new CountDownLatch(1);
        final CountDownLatch secondLatch = new CountDownLatch(1);

        ExecutorService exec = PTExecutors.newFixedThreadPool(2);


        Future<?> firstFuture = exec.submit(() -> {
            try {
                txManager.runTaskThrowOnConflict(t -> {
                    twoConflictingTasks.startFirstAndFail(t, streamId);
                    letOtherTaskFinish(firstLatch, secondLatch);
                    return null;
                });
                fail("Because we concurrently wrote, we should have failed with TransactionConflictException.");
            } catch (TransactionConflictException e) {
                // expected
            }
        });

        firstLatch.await();

        Future<?> secondFuture = exec.submit((Runnable) () -> txManager.runTaskThrowOnConflict((TransactionTask<Void, RuntimeException>) t -> {
            twoConflictingTasks.startSecondAndFinish(t, streamId);
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
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    abstract class TwoConflictingTasks {
        public abstract void startFirstAndFail(Transaction t, long streamId);
        public abstract void startSecondAndFinish(Transaction t, long streamId);
    }

}
