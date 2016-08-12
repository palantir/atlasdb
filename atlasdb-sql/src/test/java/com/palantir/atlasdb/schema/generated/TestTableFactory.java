package com.palantir.atlasdb.schema.generated;

import java.util.List;

import javax.annotation.Generated;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.table.generation.Triggers;
import com.palantir.atlasdb.transaction.api.Transaction;

@Generated("com.palantir.atlasdb.table.description.render.TableFactoryRenderer")
public final class TestTableFactory {
    private final static Namespace defaultNamespace = Namespace.create("test", Namespace.UNCHECKED_NAME);
    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;
    private final Namespace namespace;

    public static TestTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        return new TestTableFactory(sharedTriggers, namespace);
    }

    public static TestTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new TestTableFactory(sharedTriggers, defaultNamespace);
    }

    private TestTableFactory(List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        this.sharedTriggers = sharedTriggers;
        this.namespace = namespace;
    }

    public static TestTableFactory of(Namespace namespace) {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), namespace);
    }

    public static TestTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), defaultNamespace);
    }

    public DynamicTableTable getDynamicTableTable(Transaction t, DynamicTableTable.DynamicTableTrigger... triggers) {
        return DynamicTableTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public OnlyTableTable getOnlyTableTable(Transaction t, OnlyTableTable.OnlyTableTrigger... triggers) {
        return OnlyTableTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers extends
            DynamicTableTable.DynamicTableTrigger,
            OnlyTableTable.OnlyTableTrigger {
        /* empty */
    }

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putDynamicTable(Multimap<DynamicTableTable.DynamicTableRow, ? extends DynamicTableTable.DynamicTableColumnValue> newRows) {
            // do nothing
        }

        @Override
        public void putOnlyTable(Multimap<OnlyTableTable.OnlyTableRow, ? extends OnlyTableTable.OnlyTableNamedColumnValue<?>> newRows) {
            // do nothing
        }
    }
}