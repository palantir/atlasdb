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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbTestCase;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.schema.stream.generated.StreamTestStreamStore;
import com.palantir.atlasdb.schema.stream.generated.StreamTestTableFactory;
import com.palantir.atlasdb.schema.stream.generated.StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow;
import com.palantir.atlasdb.schema.stream.generated.StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow;
import com.palantir.atlasdb.schema.stream.generated.StreamTestWithHashStreamStore;
import com.palantir.atlasdb.schema.stream.generated.StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueRow;
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
        storeAndCheckByteStream(0);
        storeAndCheckByteStream(100);
        storeAndCheckByteStream(StreamTestStreamStore.BLOCK_SIZE_IN_BYTES + 500);
        storeAndCheckByteStream(StreamTestStreamStore.BLOCK_SIZE_IN_BYTES * 3);
        storeAndCheckByteStream(5000000);
    }

    private long storeAndCheckByteStream(int size) throws IOException {
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
        InputStream stream = txManager.runTaskThrowOnConflict(t -> store.loadStream(t, id));

        Sha256Hash hash1 = Sha256Hash.computeHash(bytesToStore);
        Sha256Hash hash2 = Sha256Hash.computeHash(IOUtils.toByteArray(stream));
        Assert.assertEquals(hash1, hash2);
        return id;
    }

    @Test
    public void testExpiringStoreByteStream() throws IOException {
        storeAndCheckByteStreamExpiring(0);
        storeAndCheckByteStreamExpiring(100);
        storeAndCheckByteStreamExpiring(StreamTestStreamStore.BLOCK_SIZE_IN_BYTES + 500);
        storeAndCheckByteStreamExpiring(StreamTestStreamStore.BLOCK_SIZE_IN_BYTES * 3);
        storeAndCheckByteStreamExpiring(5000000);
    }

    private long storeAndCheckByteStreamExpiring(int size) throws IOException {
        final byte[] bytesToStore = new byte[size];
        Random rand = new Random();
        rand.nextBytes(bytesToStore);

        final long id = timestampService.getFreshTimestamp();
        StreamTestWithHashStreamStore store = StreamTestWithHashStreamStore.of(txManager, StreamTestTableFactory.of());

        txManager.runTaskWithRetry(t -> {
            store.storeStream(id, new ByteArrayInputStream(bytesToStore), 5, TimeUnit.SECONDS);
            return null;
        });
        InputStream stream = txManager.runTaskThrowOnConflict(t -> store.loadStream(t, id));

        Sha256Hash hash1 = Sha256Hash.computeHash(bytesToStore);
        Sha256Hash hash2 = Sha256Hash.computeHash(IOUtils.toByteArray(stream));
        Assert.assertEquals(hash1, hash2);
        return id;
    }
}
