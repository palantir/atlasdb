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
package com.palantir.atlasdb.schema;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.AtlasDbTestCase;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.junit.Before;
import org.junit.Test;

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

        txManager.runTaskWithRetry((TransactionTask<Void, RuntimeException>) txn -> {
            verifyCheckpoints(t1, startById1, txn);
            verifyCheckpoints(t2, startById2, txn);
            return null;
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
        txManager.runTaskWithRetry((TransactionTask<Void, RuntimeException>) txn -> {
            for (Map.Entry<Long, byte[]> e : next1.entrySet()) {
                checkpointer.checkpoint(t1, e.getKey(), e.getValue(), txn);
            }
            for (Map.Entry<Long, byte[]> e : next2.entrySet()) {
                checkpointer.checkpoint(t2, e.getKey(), e.getValue(), txn);
            }
            return null;
        });

        txManager.runTaskWithRetry((TransactionTask<Void, RuntimeException>) txn -> {
            verifyCheckpoints(t1, next1, txn);
            verifyCheckpoints(t2, next2, txn);
            return null;
        });
    }

    @Test
    public void testEnd() {
        final String t1 = "t1";

        final Map<Long, byte[]> startById1 = createRandomCheckpoints();
        checkpointer.createCheckpoints(t1, startById1);

        final Map<Long, byte[]> next1 = createRandomCheckpoints();
        txManager.runTaskWithRetry((TransactionTask<Void, RuntimeException>) txn -> {
            for (Map.Entry<Long, byte[]> e : next1.entrySet()) {
                checkpointer.checkpoint(t1, e.getKey(), new byte[0], txn);
            }
            return null;
        });

        txManager.runTaskWithRetry((TransactionTask<Void, RuntimeException>) txn -> {
            for (long rangeId : next1.keySet()) {
                byte[] oldCheckpoint = checkpointer.getCheckpoint(t1, rangeId, txn);
                assertThat(oldCheckpoint).isNull();
            }
            return null;
        });
    }

    private Map<Long, byte[]> createRandomCheckpoints() {
        byte[] bytes = new byte[64];
        Random random = new Random();
        Map<Long, byte[]> ret = new HashMap<>();
        for (int i = 0; i < 4; i++) {
            random.nextBytes(bytes);
            ret.put((long) i, bytes.clone());
        }
        return ret;
    }

    private void verifyCheckpoints(final String extraId, final Map<Long, byte[]> startById, Transaction txn) {
        for (Map.Entry<Long, byte[]> e : startById.entrySet()) {
            byte[] oldCheckpoint = checkpointer.getCheckpoint(extraId, e.getKey(), txn);
            byte[] currentCheckpoint = e.getValue();
            assertThat(oldCheckpoint).isEqualTo(currentCheckpoint);
        }
    }
}
