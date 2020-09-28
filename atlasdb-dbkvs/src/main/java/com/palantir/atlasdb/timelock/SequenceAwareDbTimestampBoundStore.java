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

package com.palantir.atlasdb.timelock;

import java.util.OptionalLong;

import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.nexus.db.pool.ConnectionManager;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;

public final class SequenceAwareDbTimestampBoundStore implements TimestampBoundStore {
    private final DbBoundTransactionManager dbBoundTransactionManager;

    private SequenceAwareDbTimestampBoundStore(DbBoundTransactionManager dbBoundTransactionManager) {
        this.dbBoundTransactionManager = dbBoundTransactionManager;
        dbBoundTransactionManager.runTaskTransactionScoped(store -> {
            store.createTimestampTable();
            return null;
        });
    }

    public static TimestampBoundStore create(ConnectionManager connectionManager, String client) {
        return new SequenceAwareDbTimestampBoundStore(
                DbBoundTransactionManager.create(connectionManager, client));
    }

    @Override
    public synchronized long getUpperLimit() {
        return dbBoundTransactionManager.runTaskTransactionScoped(store -> {
            OptionalLong currentValue = store.read();
            if (currentValue.isPresent()) {
                return currentValue.getAsLong();
            }

            store.initialize(1_000_000);
            return store.read().orElseThrow(() -> new SafeIllegalStateException("Unexpectedly read empty store"));
        });
    }

    @Override
    public synchronized void storeUpperLimit(long limit) {
        return dbBoundTransactionManager.runTaskTransactionScoped(store -> {

        })
    }
}
