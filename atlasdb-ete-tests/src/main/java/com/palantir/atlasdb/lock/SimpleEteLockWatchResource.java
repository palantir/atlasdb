/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.lock;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.watch.ExposedLockWatchManager;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.watch.CommitUpdate;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class SimpleEteLockWatchResource implements EteLockWatchResource {
    private static final TableReference LOCK_WATCH_TABLE = TableReference.createFromFullyQualifiedName("lock.watch");

    private final TransactionManager transactionManager;

    public SimpleEteLockWatchResource(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    @Override
    public void writeTransaction(Map<Cell, String> values) {
        transactionManager.runTaskWithRetry(tx -> {
            Map<Cell, byte[]> byteValues = KeyedStream.stream(values)
                    .map(PtBytes::toBytes)
                    .collectToMap();
            tx.put(LOCK_WATCH_TABLE, byteValues);
            return null;
        });
    }

    @Override
    public CommitUpdate getCommitUpdate() {
        AtomicReference<CommitUpdate> commitUpdate = new AtomicReference<>();
        transactionManager.runTaskWithConditionWithRetry(
                () -> ts -> commitUpdate.set(
                        new ExposedLockWatchManager(transactionManager.getLockWatchManager()).getCommitUpdate(ts)),
                (tx, condition) -> null);
        return commitUpdate.get();
    }
}
