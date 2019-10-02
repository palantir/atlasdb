package com.palantir.atlasdb.migration.generated;

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
public final class MigrationProgressTableFactory {
    private static final Namespace defaultNamespace = Namespace.create("migration", Namespace.UNCHECKED_NAME);

    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;

    private final Namespace namespace;

    private MigrationProgressTableFactory(
            List<Function<? super Transaction, SharedTriggers>> sharedTriggers,
            Namespace namespace) {
        this.sharedTriggers = sharedTriggers;
        this.namespace = namespace;
    }

    public static MigrationProgressTableFactory of(
            List<Function<? super Transaction, SharedTriggers>> sharedTriggers,
            Namespace namespace) {
        return new MigrationProgressTableFactory(sharedTriggers, namespace);
    }

    public static MigrationProgressTableFactory of(
            List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new MigrationProgressTableFactory(sharedTriggers, defaultNamespace);
    }

    public static MigrationProgressTableFactory of(Namespace namespace) {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), namespace);
    }

    public static MigrationProgressTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), defaultNamespace);
    }

    public ProgressTable getProgressTable(Transaction t,
            ProgressTable.ProgressTrigger... triggers) {
        return ProgressTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers extends ProgressTable.ProgressTrigger {
    }

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putProgress(
                Multimap<ProgressTable.ProgressRow, ? extends ProgressTable.ProgressNamedColumnValue<?>> newRows) {
            // do nothing
        }
    }
}
