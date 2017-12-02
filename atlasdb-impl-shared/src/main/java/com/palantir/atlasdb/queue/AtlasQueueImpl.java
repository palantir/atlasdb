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

import java.util.Optional;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.schema.queue.AtlasQueue;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.StringLockDescriptor;

public class AtlasQueueImpl implements AtlasQueue {

    private final LockAwareTransactionManager txMgr;
    private final TableReference queueTable;

    public AtlasQueueImpl(LockAwareTransactionManager txMgr, TableReference queueTable,
            TableReference queueReadOffsetTable, TableReference queueWriteOffsetTable) {
        this.txMgr = txMgr;
        this.queueTable = queueTable;
    }

    @Override
    public void enqueue(byte[] queueKey, byte[] value) {
        try {
            txMgr.runTaskWithLocksWithRetry(
                    () -> LockRequest.builder(ImmutableSortedMap.of(
                            StringLockDescriptor.of("queue__" + BaseEncoding.base64().encode(queueKey)), LockMode.WRITE
                    )).build(),
                    (tx, heldLocks) -> {
                        Cell queueOffsetCell = Cell.create(queueKey, PtBytes.toBytes("o"));
                        long currentOffset = getCurrentOffset(tx, queueWriteOffsetTable, queueOffsetCell);

                        long bucketNumber = currentOffset / MAX_ENTRIES_PER_BUCKET;
                        byte[] delegateQueue =
                                EncodingUtils.add(queueKey, EncodingUtils.encodeUnsignedVarLong(bucketNumber));

                        tx.put(queueTable, ImmutableMap.of(
                                Cell.create(delegateQueue, EncodingUtils.encodeUnsignedVarLong(currentOffset)),
                                value));
                        tx.put(queueWriteOffsetTable, ImmutableMap.of(
                                queueOffsetCell,
                                EncodingUtils.encodeUnsignedVarLong(currentOffset + 1)));
                        return null;
                    });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
    }


    @Override
    public Optional<byte[]> dequeue(byte[] queueKey) {
        try {
            return txMgr.runTaskWithLocksWithRetry(
                    () -> LockRequest.builder(ImmutableSortedMap.of(
                            StringLockDescriptor.of("queue__" + BaseEncoding.base64().encode(queueKey)), LockMode.WRITE
                    )).build(),
                    (tx, lockTokens) -> {
                        Cell queueOffsetCell = Cell.create(queueKey, PtBytes.toBytes("o"));
                        long currentReadOffset = getCurrentOffset(tx, queueReadOffsetTable, queueOffsetCell);
                        long currentWriteOffset = getCurrentOffset(tx, queueWriteOffsetTable, queueOffsetCell);
                        if (currentReadOffset == currentWriteOffset) {
                            return Optional.empty();
                        }

                        long bucketNumber = currentReadOffset / MAX_ENTRIES_PER_BUCKET;
                        byte[] delegateQueue =
                                EncodingUtils.add(queueKey, EncodingUtils.encodeUnsignedVarLong(bucketNumber));

                        // There is something to read
                        tx.get(queueReadOffsetTable,
                                ImmutableSet.of(Cell.create(delegateQueue, EncodingUtils.encodeUnsignedVarLong(currentReadOffset))));

                        return Optional.empty();
                    });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
    }
}
