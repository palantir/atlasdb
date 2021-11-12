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
package com.palantir.atlasdb.sweep.queue;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import java.util.List;

public class TargetedSweepFollower {
    private final List<Follower> followers;
    private final TransactionManager txManager;

    /**
     * A follower wrapper for targeted sweep that gets initialized once the TransactionManager is available and
     * always runs on the same manager and with Transaction.TransactionType.HARD_DELETE.
     * @param followers followers to wrap
     * @param txManager fixed transaction manager
     */
    public TargetedSweepFollower(List<Follower> followers, TransactionManager txManager) {
        this.followers = followers;
        this.txManager = txManager;
    }

    public void run(TableReference tableRef, Multimap<Cell, Long> cells) {
        followers.forEach(flwr -> flwr.run(txManager, tableRef, cells, Transaction.TransactionType.HARD_DELETE));
    }
}
