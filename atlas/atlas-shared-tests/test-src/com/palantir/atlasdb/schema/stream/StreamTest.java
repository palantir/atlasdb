// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.schema.stream;

import java.io.ByteArrayInputStream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbTestCase;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.schema.stream.generated.StreamTestStreamStore;
import com.palantir.atlasdb.schema.stream.generated.StreamTestTableFactory;
import com.palantir.atlasdb.stream.PersistentStreamStore;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionTask;

public class StreamTest extends AtlasDbTestCase {

    @Before
    public void createSchema() {
        StreamTestSchema.getSchema().deleteTablesAndIndexes(keyValueService);
        StreamTestSchema.getSchema().createTablesAndIndexes(keyValueService);
    }

    @Test
    public void testAddDelete() throws Exception {
        txManager.runTaskWithRetry(new TransactionTask<Void, Exception>() {
            @Override
            public Void execute(Transaction t) throws Exception {
                PersistentStreamStore<Long> store = StreamTestStreamStore.of(t, StreamTestTableFactory.of());
                byte[] data = PtBytes.toBytes("streamed");
                store.storeStream(1L, new ByteArrayInputStream(data));
                Assert.assertEquals(data.length, store.loadStream(1L).read(data, 0, data.length));
                byte[] reference = "ref".getBytes();
                store.markStreamsAsUsed(ImmutableMap.of(1L, reference));
                store.removeStreamsAsUsed(ImmutableMap.of(1L, reference));
                try {
                    store.loadStream(1L).read(data, 0, data.length);
                } catch (IllegalArgumentException e) {
                    // expected
                    return null;
                }
                Assert.fail();
                return null;
            }
        });
    }
}
