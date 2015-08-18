package com.palantir.atlasdb.schema.indexing.generated;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.table.generation.Triggers;
import com.palantir.atlasdb.transaction.api.Transaction;

public class IndexTestTableFactory {
    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;

    public static IndexTestTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new IndexTestTableFactory(sharedTriggers);
    }

    private IndexTestTableFactory(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        this.sharedTriggers = sharedTriggers;
    }

    public static IndexTestTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of());
    }

    public DataTable getDataTable(Transaction t, DataTable.DataTrigger... triggers) {
        return DataTable.of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public TwoColumnsTable getTwoColumnsTable(Transaction t, TwoColumnsTable.TwoColumnsTrigger... triggers) {
        return TwoColumnsTable.of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers extends
            DataTable.DataTrigger,
            TwoColumnsTable.TwoColumnsTrigger {
        /* empty */
    }

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putData(Multimap<DataTable.DataRow, ? extends DataTable.DataNamedColumnValue<?>> newRows) {
            // do nothing
        }

        @Override
        public void putTwoColumns(Multimap<TwoColumnsTable.TwoColumnsRow, ? extends TwoColumnsTable.TwoColumnsNamedColumnValue<?>> newRows) {
            // do nothing
        }
    }
}