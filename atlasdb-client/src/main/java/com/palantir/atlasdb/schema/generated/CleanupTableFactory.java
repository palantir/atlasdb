package com.palantir.atlasdb.schema.generated;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.table.generation.Triggers;
import com.palantir.atlasdb.transaction.api.Transaction;
import java.lang.Override;
import java.util.List;
import javax.annotation.Generated;

@Generated("com.palantir.atlasdb.table.description.render.TableFactoryRenderer")
public final class CleanupTableFactory {
    private static final Namespace defaultNamespace = Namespace.create("cleanup", Namespace.UNCHECKED_NAME);

    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;

    private final Namespace namespace;

    private CleanupTableFactory(List<Function<? super Transaction, SharedTriggers>> sharedTriggers,
            Namespace namespace) {
        this.sharedTriggers = sharedTriggers;
        this.namespace = namespace;
    }

    public static CleanupTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers,
            Namespace namespace) {
        return new CleanupTableFactory(sharedTriggers, namespace);
    }

    public static CleanupTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new CleanupTableFactory(sharedTriggers, defaultNamespace);
    }

    public static CleanupTableFactory of(Namespace namespace) {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), namespace);
    }

    public static CleanupTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), defaultNamespace);
    }

    public CleanupQueueTable getCleanupQueueTable(Transaction t,
            CleanupQueueTable.CleanupQueueTrigger... triggers) {
        return CleanupQueueTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public CleanupReadOffsetsTable getCleanupReadOffsetsTable(Transaction t,
            CleanupReadOffsetsTable.CleanupReadOffsetsTrigger... triggers) {
        return CleanupReadOffsetsTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public CleanupWriteOffsetsTable getCleanupWriteOffsetsTable(Transaction t,
            CleanupWriteOffsetsTable.CleanupWriteOffsetsTrigger... triggers) {
        return CleanupWriteOffsetsTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers extends CleanupQueueTable.CleanupQueueTrigger, CleanupReadOffsetsTable.CleanupReadOffsetsTrigger, CleanupWriteOffsetsTable.CleanupWriteOffsetsTrigger {
    }

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putCleanupQueue(Multimap<CleanupQueueTable.CleanupQueueRow, ? extends CleanupQueueTable.CleanupQueueColumnValue> newRows) {
            // do nothing
        }

        @Override
        public void putCleanupReadOffsets(Multimap<CleanupReadOffsetsTable.CleanupReadOffsetsRow, ? extends CleanupReadOffsetsTable.CleanupReadOffsetsNamedColumnValue<?>> newRows) {
            // do nothing
        }

        @Override
        public void putCleanupWriteOffsets(Multimap<CleanupWriteOffsetsTable.CleanupWriteOffsetsRow, ? extends CleanupWriteOffsetsTable.CleanupWriteOffsetsNamedColumnValue<?>> newRows) {
            // do nothing
        }
    }
}
