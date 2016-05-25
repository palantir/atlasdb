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
package com.palantir.atlasdb.schema;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.AtlasDbTestCase;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionTask;

public abstract class AbstractTaskCheckpointerTest extends AtlasDbTestCase {
    protected AbstractTaskCheckpointer checkpointer;

    protected abstract AbstractTaskCheckpointer getCheckpointer();

    @Before
    public void createCheckpointer() {
        checkpointer = getCheckpointer();
    }

    @Test
    public void testInitialize() {
        final String t1 = "t1";
        final String t2 = "t2";

        final Map<Long, byte[]> startById1 = createRandomCheckpoints();
        final Map<Long, byte[]> startById2 = createRandomCheckpoints();
        checkpointer.createCheckpoints(t1, startById1);
        checkpointer.createCheckpoints(t2, startById2);

        txManager.runTaskWithRetry(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction t) {
                verifyCheckpoints(t1, startById1, t);
                verifyCheckpoints(t2, startById2, t);
                return null;
            }
        });
    }

    @Test
    public void testCheckpoint() {
        final String t1 = "t1";
        final String t2 = "t2";

        final Map<Long, byte[]> startById1 = createRandomCheckpoints();
        final Map<Long, byte[]> startById2 = createRandomCheckpoints();
        checkpointer.createCheckpoints(t1, startById1);
        checkpointer.createCheckpoints(t2, startById2);

        final Map<Long, byte[]> next1 = createRandomCheckpoints();
        final Map<Long, byte[]> next2 = createRandomCheckpoints();
        txManager.runTaskWithRetry(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction t) {
                for (Entry<Long, byte[]> e : next1.entrySet()) {
                    checkpointer.checkpoint(t1, e.getKey(), e.getValue(), t);
                }
                for (Entry<Long, byte[]> e : next2.entrySet()) {
                    checkpointer.checkpoint(t2, e.getKey(), e.getValue(), t);
                }
                return null;
            }
        });

        txManager.runTaskWithRetry(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction t) {
                verifyCheckpoints(t1, next1, t);
                verifyCheckpoints(t2, next2, t);
                return null;
            }
        });
    }

    @Test
    public void testEnd() {
        final String t1 = "t1";

        final Map<Long, byte[]> startById1 = createRandomCheckpoints();
        checkpointer.createCheckpoints(t1, startById1);

        final Map<Long, byte[]> next1 = createRandomCheckpoints();
        txManager.runTaskWithRetry(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction t) {
                for (Entry<Long, byte[]> e : next1.entrySet()) {
                    checkpointer.checkpoint(t1, e.getKey(), new byte[0], t);
                }
                return null;
            }
        });

        txManager.runTaskWithRetry(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction t) {
                for (long rangeId : next1.keySet()) {
                    byte[] oldCheckpoint = checkpointer.getCheckpoint(t1, rangeId, t);
                    Assert.assertNull(oldCheckpoint);
                }
                return null;
            }
        });
    }

    private Map<Long, byte[]> createRandomCheckpoints() {
        byte[] bytes = new byte[64];
        Random r = new Random();
        Map<Long, byte[]> ret = Maps.newHashMap();
        for (int i = 0; i < 4; i++) {
            r.nextBytes(bytes);
            ret.put((long) i, bytes.clone());
        }
        return ret;
    }

    private void verifyCheckpoints(final String extraId,
                                   final Map<Long, byte[]> startById,
                                   Transaction t) {
        for (Entry<Long, byte[]> e : startById.entrySet()) {
            byte[] oldCheckpoint = checkpointer.getCheckpoint(extraId, e.getKey(), t);
            byte[] currentCheckpoint = e.getValue();
            Assert.assertTrue(Arrays.equals(oldCheckpoint, currentCheckpoint));
        }
    }
}
