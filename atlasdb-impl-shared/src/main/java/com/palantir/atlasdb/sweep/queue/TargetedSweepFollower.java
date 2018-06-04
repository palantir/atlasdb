/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

import java.util.Set;

import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;

public class TargetedSweepFollower {
    private final Follower follower;
    private final TransactionManager txManager;

    public TargetedSweepFollower(Follower follower, TransactionManager txManager) {
        this.follower = follower;
        this.txManager = txManager;
    }

    public void run(TableReference tableRef, Set<Cell> cells) {
        follower.run(txManager, tableRef, cells, Transaction.TransactionType.HARD_DELETE);
    }
}
