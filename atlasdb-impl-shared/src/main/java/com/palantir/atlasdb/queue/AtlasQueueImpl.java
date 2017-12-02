/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.queue;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.queue.AtlasQueue;
import com.palantir.atlasdb.schema.queue.ImmutableQueueHeadResult;
import com.palantir.atlasdb.schema.queue.QueueHeadResult;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.StringLockDescriptor;

public class AtlasQueueImpl implements AtlasQueue {
    private static final BiFunction<byte[], LockMode, LockRequest> LOCK_REQUEST_FUNCTION = (key, lockMode) ->
            LockRequest.builder(ImmutableSortedMap.of(
                    StringLockDescriptor.of("_queue" + BaseEncoding.base64().encode(key)), lockMode))
                    .build();

    private final LockAwareTransactionManager txMgr;
    private final TableReference queueTable;
    private final OffsetManager offsetManager;
    private final byte[] queueKey;

    private AtlasQueueImpl(
            LockAwareTransactionManager txMgr,
            TableReference queueTable,
            OffsetManager offsetManager,
            byte[] queueKey) {
        this.txMgr = txMgr;
        this.queueTable = queueTable;
        this.offsetManager = offsetManager;
        this.queueKey = queueKey;
    }

    @Override
    public void enqueue(byte[] value) {
        try {
            txMgr.runTaskWithLocksWithRetry(() -> LOCK_REQUEST_FUNCTION.apply(queueKey, LockMode.WRITE),
                    (tx, lockTokens) -> {
                        QueueQuery query = offsetManager.translateWriteQuery(tx);
                        tx.put(queueTable, ImmutableMap.of(query.toCell(), value));
                        offsetManager.updateWriteOffsetPast(tx, query.offset());
                        return null;
                    });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Optional<QueueHeadResult> head() {
        try {
            return txMgr.runTaskWithLocksWithRetry(() -> LOCK_REQUEST_FUNCTION.apply(queueKey, LockMode.READ),
                    (tx, lockTokens) -> {
                        Optional<QueueQuery> queryOptional = offsetManager.translateReadQuery(tx);
                        return queryOptional.flatMap(query -> {
                            Map<Cell, byte[]> response = tx.get(queueTable, ImmutableSet.of(query.toCell()));
                            if (response.isEmpty()) {
                                // TODO (jkong): Think if this is the right thing to do.
                                return Optional.empty();
                            }
                            return Optional.of(ImmutableQueueHeadResult.of(query.offset(), response.get(query.toCell())));
                        });
                    });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void flush(byte[] offset) {
        try {
            txMgr.runTaskWithLocksWithRetry(() -> LOCK_REQUEST_FUNCTION.apply(queueKey, LockMode.WRITE),
                    (tx, lockTokens) -> {
                        offsetManager.updateReadOffsetPast(tx, offset);
                        tx.delete(queueTable, ImmutableSet.of(Cell.create(queueKey, offset)));
                        return null;
                    });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
    }
}
