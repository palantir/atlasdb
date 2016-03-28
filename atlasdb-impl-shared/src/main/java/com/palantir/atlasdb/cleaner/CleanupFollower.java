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
package com.palantir.atlasdb.cleaner;

import java.util.Collection;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.Transaction.TransactionType;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;

public class CleanupFollower implements Follower {
    private final ImmutableMultimap<TableReference, OnCleanupTask> cleanupTasksByTable;
    public static CleanupFollower create(Iterable<Schema> schemas) {
        ImmutableMultimap.Builder<TableReference, OnCleanupTask> cleanupTasksByTable = ImmutableMultimap.builder();
        for (Schema schema : schemas) {
            cleanupTasksByTable.putAll(schema.getCleanupTasksByTable());
        }
        return new CleanupFollower(cleanupTasksByTable.build());
    }

    public static CleanupFollower create(Schema schema) {
        return new CleanupFollower(schema.getCleanupTasksByTable());
    }

    private CleanupFollower(Multimap<TableReference, OnCleanupTask> cleanupTasksByTable) {
        this.cleanupTasksByTable = ImmutableMultimap.copyOf(cleanupTasksByTable);
    }

    @Override
    public void run(TransactionManager txManager, TableReference tableRef, final Set<Cell> cells, final TransactionType transactionType) {
        Collection<OnCleanupTask> nextTasks = cleanupTasksByTable.get(tableRef);
        while (!nextTasks.isEmpty()) {
            final Collection<OnCleanupTask> cleanupTasks = nextTasks;
            nextTasks = txManager.runTaskWithRetry(new TransactionTask<Collection<OnCleanupTask>, RuntimeException>() {
                @Override
                public Collection<OnCleanupTask> execute(Transaction t) {
                    Collection<OnCleanupTask> toRetry = Lists.newArrayList();
                    Preconditions.checkArgument(
                            transactionType == TransactionType.HARD_DELETE ||
                            transactionType == TransactionType.AGGRESSIVE_HARD_DELETE);
                    t.setTransactionType(transactionType);
                    for (OnCleanupTask task : cleanupTasks) {
                        boolean needsRetry = task.cellsCleanedUp(t, cells);
                        if (needsRetry) {
                            toRetry.add(task);
                        }
                    }
                    return toRetry;
                }
            });
        }
    }
}
