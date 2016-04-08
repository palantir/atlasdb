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
import static org.junit.Assert.fail;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.AtlasDbTestCase;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.schema.stream.generated.DeletingStreamStore;
import com.palantir.atlasdb.schema.stream.generated.StreamTestStreamStore;
import com.palantir.atlasdb.schema.stream.generated.StreamTestTableFactory;
import com.palantir.atlasdb.schema.stream.generated.StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow;
import com.palantir.atlasdb.schema.stream.generated.StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow;
import com.palantir.atlasdb.schema.stream.generated.StreamTestWithHashStreamStore;
import com.palantir.atlasdb.schema.stream.generated.StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueRow;
import com.palantir.atlasdb.stream.GenericStreamStore;
import com.palantir.atlasdb.stream.PersistentStreamStore;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionConflictException;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.util.Pair;
import com.palantir.util.crypto.Sha256Hash;

public class StreamTest extends AtlasDbTestCase {

    @Before
    public void createSchema() {
        Schemas.deleteTablesAndIndexes(StreamTestSchema.getSchema(), keyValueService);
        Schemas.createTablesAndIndexes(StreamTestSchema.getSchema(), keyValueService);
    }

    @Test
    public void testAddDelete() throws Exception {
        final byte[] data = PtBytes.toBytes("streamed");
        final long streamId = txManager.runTaskWithRetry(new TransactionTask<Long, Exception>() {
            @Override
            public Long execute(Transaction t) throws Exception {
                PersistentStreamStore store = StreamTestStreamStore.of(txManager, StreamTestTableFactory.of());
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
                PersistentStreamStore store = StreamTestStreamStore.of(txManager, StreamTestTableFactory.of());
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
    public void testStoreByteStream() throws IOException {
        storeAndCheckByteStreams(0);
        storeAndCheckByteStreams(100);
        storeAndCheckByteStreams(StreamTestStreamStore.BLOCK_SIZE_IN_BYTES + 500);
        storeAndCheckByteStreams(StreamTestStreamStore.BLOCK_SIZE_IN_BYTES * 3);
        storeAndCheckByteStreams(5000000);
    }

    private long storeAndCheckByteStreams(int size) throws IOException {
        byte[] reference = PtBytes.toBytes("ref");
        final byte[] bytesToStore = new byte[size];
        Random rand = new Random();
        rand.nextBytes(bytesToStore);

        final long id = timestampService.getFreshTimestamp();
        PersistentStreamStore store = StreamTestStreamStore.of(txManager, StreamTestTableFactory.of());
        txManager.runTaskWithRetry(t -> {
                    store.storeStreams(t, ImmutableMap.of(id, new ByteArrayInputStream(bytesToStore)));
                    store.markStreamAsUsed(t, id, reference);
                    return null;
                });

        verifyLoadingStreams(id, bytesToStore, store);

        store.storeStream(new ByteArrayInputStream(bytesToStore));
        verifyLoadingStreams(id, bytesToStore, store);

        return id;
    }

    @Test
    public void testExpiringStoreByteStream() throws IOException {
        storeAndCheckExpiringByteStreams(0);
        storeAndCheckExpiringByteStreams(100);
        storeAndCheckExpiringByteStreams(StreamTestStreamStore.BLOCK_SIZE_IN_BYTES + 500);
        storeAndCheckExpiringByteStreams(StreamTestStreamStore.BLOCK_SIZE_IN_BYTES * 3);
        storeAndCheckExpiringByteStreams(5000000);
    }

    private long storeAndCheckExpiringByteStreams(int size) throws IOException {
        final byte[] bytesToStore = new byte[size];
        Random rand = new Random();
        rand.nextBytes(bytesToStore);

        final long id = timestampService.getFreshTimestamp();
        StreamTestWithHashStreamStore store = StreamTestWithHashStreamStore.of(txManager, StreamTestTableFactory.of());
        store.storeStream(id, new ByteArrayInputStream(bytesToStore), 5, TimeUnit.SECONDS);

        verifyLoadingStreams(id, bytesToStore, store);

        return id;
    }

    private void verifyLoadingStreams(long id, byte[] bytesToStore, GenericStreamStore<Long> store) throws IOException {
        verifyLoadStream(id, bytesToStore, store);
        verifyLoadStreams(id, bytesToStore, store);
        verifyLoadStreamAsFile(id, bytesToStore, store);
    }

    private void verifyLoadStreamAsFile(long id, byte[] bytesToStore, GenericStreamStore<Long> store) throws IOException {
        File file = txManager.runTaskThrowOnConflict(t -> store.loadStreamAsFile(t, id));
        Assert.assertArrayEquals(bytesToStore, FileUtils.readFileToByteArray(file));
    }

    private void verifyLoadStreams(long id, byte[] bytesToStore, GenericStreamStore<Long> store) throws IOException {
        Map<Long, InputStream> streams = txManager.runTaskThrowOnConflict(t ->
                store.loadStreams(t, ImmutableSet.of(id)));
        assertStreamHasBytes(streams.get(id), bytesToStore);
    }

    private void verifyLoadStream(long id, byte[] bytesToStore, GenericStreamStore<Long> store) throws IOException {
        InputStream stream = txManager.runTaskThrowOnConflict(t -> store.loadStream(t, id));
        assertStreamHasBytes(stream, bytesToStore);
    }

    private void assertStreamHasBytes(InputStream stream, byte[] bytes) throws IOException {
        byte[] streamAsBytes = IOUtils.toByteArray(stream);
        Assert.assertArrayEquals(bytes, streamAsBytes);
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

        PersistentStreamStore store = StreamTestStreamStore.of(txManager, StreamTestTableFactory.of());

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

        PersistentStreamStore store = StreamTestStreamStore.of(txManager, StreamTestTableFactory.of());
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
