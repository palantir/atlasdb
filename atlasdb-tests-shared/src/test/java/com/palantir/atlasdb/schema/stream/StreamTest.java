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
import java.util.NoSuchElementException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.AtlasDbTestCase;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.schema.stream.generated.StreamTestStreamStore;
import com.palantir.atlasdb.schema.stream.generated.StreamTestTableFactory;
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
}
