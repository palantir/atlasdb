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

import static junit.framework.TestCase.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbTestCase;
import com.palantir.atlasdb.encoding.PtBytes;
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
import com.palantir.atlasdb.transaction.api.TransactionTask;
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
        verifyLoadingStreamsAsFile(id, bytesToStore, store);

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
        verifyLoadingStreamsAsFile(id, bytesToStore, store);

        return id;
    }

    private void verifyLoadingStreams(long id, byte[] bytesToStore, GenericStreamStore<Long> store) throws IOException {
        byte[] loadedBytes;
        InputStream stream = txManager.runTaskThrowOnConflict(t -> store.loadStream(t, id));

        Sha256Hash expectedHash = Sha256Hash.computeHash(bytesToStore);
        loadedBytes = IOUtils.toByteArray(stream);
        Assert.assertArrayEquals(bytesToStore, loadedBytes);
        Assert.assertEquals(expectedHash, Sha256Hash.computeHash(loadedBytes));

        Map<Long, InputStream> streams = txManager.runTaskThrowOnConflict(t ->
                store.loadStreams(t, ImmutableSet.of(id)));

        loadedBytes = IOUtils.toByteArray(streams.get(id));
        Assert.assertArrayEquals(bytesToStore, loadedBytes);
        Assert.assertEquals(expectedHash, Sha256Hash.computeHash(loadedBytes));
    }

    private void verifyLoadingStreamsAsFile(long id, byte[] bytesToStore, GenericStreamStore<Long> store) throws IOException {
        byte[] loadedBytes;
        Sha256Hash expectedHash = Sha256Hash.computeHash(bytesToStore);

        File file = txManager.runTaskThrowOnConflict(t -> store.loadStreamAsFile(t, id));
        loadedBytes = FileUtils.readFileToByteArray(file);
        Assert.assertArrayEquals(bytesToStore, loadedBytes);
        Assert.assertEquals(expectedHash, Sha256Hash.computeHash(loadedBytes));
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

}
