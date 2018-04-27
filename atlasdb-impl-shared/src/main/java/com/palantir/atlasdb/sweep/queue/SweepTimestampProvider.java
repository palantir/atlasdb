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

package com.palantir.atlasdb.sweep.queue;

import java.util.function.LongSupplier;

import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.atlasdb.transaction.api.TransactionManager;

public class SweepTimestampProvider {

    private final LongSupplier unreadableTimestamp;
    private final LongSupplier immutableTimestamp;

    public static SweepTimestampProvider create(TransactionManager txnManager) {
        return new SweepTimestampProvider(txnManager::getUnreadableTimestamp, txnManager::getImmutableTimestamp);
    }

    public SweepTimestampProvider(LongSupplier unreadableTimestamp, LongSupplier immutableTimestamp) {
        this.unreadableTimestamp = unreadableTimestamp;
        this.immutableTimestamp = immutableTimestamp;
    }

    public long getSweepTimestamp(Sweeper sweeper) {
        return sweeper.getSweepTimestampSupplier().getSweepTimestamp(unreadableTimestamp, immutableTimestamp);
    }

}
