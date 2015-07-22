package com.palantir.atlasdb.cleaner;

import java.util.Collection;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.Transaction.TransactionType;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;

public class CleanupFollower implements Follower {
    private final ImmutableMultimap<String, OnCleanupTask> cleanupTasksByTable;
    public static CleanupFollower create(Iterable<Schema> schemas) {
        ImmutableMultimap.Builder<String, OnCleanupTask> cleanupTasksByTable = ImmutableMultimap.builder();
        for (Schema schema : schemas) {
            cleanupTasksByTable.putAll(schema.getCleanupTasksByTable());
        }
        return new CleanupFollower(cleanupTasksByTable.build());
    }

    public static CleanupFollower create(Schema schema) {
        return new CleanupFollower(schema.getCleanupTasksByTable());
    }

    private CleanupFollower(Multimap<String, OnCleanupTask> cleanupTasksByTable) {
        this.cleanupTasksByTable = ImmutableMultimap.copyOf(cleanupTasksByTable);
    }

    @Override
    public void run(TransactionManager txManager, String tableName, final Set<Cell> cells, final TransactionType transactionType) {
        Collection<OnCleanupTask> nextTasks = cleanupTasksByTable.get(tableName);
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
