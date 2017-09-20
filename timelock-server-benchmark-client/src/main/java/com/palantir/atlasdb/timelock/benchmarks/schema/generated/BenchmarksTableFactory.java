package com.palantir.atlasdb.timelock.benchmarks.schema.generated;

import java.util.List;

import javax.annotation.Generated;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.table.generation.Triggers;
import com.palantir.atlasdb.transaction.api.Transaction;

@Generated("com.palantir.atlasdb.table.description.render.TableFactoryRenderer")
public final class BenchmarksTableFactory {
    private final static Namespace defaultNamespace = Namespace.create("benchmarks", Namespace.UNCHECKED_NAME);
    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;
    private final Namespace namespace;

    public static BenchmarksTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        return new BenchmarksTableFactory(sharedTriggers, namespace);
    }

    public static BenchmarksTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new BenchmarksTableFactory(sharedTriggers, defaultNamespace);
    }

    private BenchmarksTableFactory(List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        this.sharedTriggers = sharedTriggers;
        this.namespace = namespace;
    }

    public static BenchmarksTableFactory of(Namespace namespace) {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), namespace);
    }

    public static BenchmarksTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), defaultNamespace);
    }

    public KvDynamicColumnsTable getKvDynamicColumnsTable(Transaction t, KvDynamicColumnsTable.KvDynamicColumnsTrigger... triggers) {
        return KvDynamicColumnsTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public KvRowsTable getKvRowsTable(Transaction t, KvRowsTable.KvRowsTrigger... triggers) {
        return KvRowsTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public MetadataTable getMetadataTable(Transaction t, MetadataTable.MetadataTrigger... triggers) {
        return MetadataTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers extends
            KvDynamicColumnsTable.KvDynamicColumnsTrigger,
            KvRowsTable.KvRowsTrigger,
            MetadataTable.MetadataTrigger {
        /* empty */
    }

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putKvDynamicColumns(Multimap<KvDynamicColumnsTable.KvDynamicColumnsRow, ? extends KvDynamicColumnsTable.KvDynamicColumnsColumnValue> newRows) {
            // do nothing
        }

        @Override
        public void putKvRows(Multimap<KvRowsTable.KvRowsRow, ? extends KvRowsTable.KvRowsNamedColumnValue<?>> newRows) {
            // do nothing
        }

        @Override
        public void putMetadata(Multimap<MetadataTable.MetadataRow, ? extends MetadataTable.MetadataNamedColumnValue<?>> newRows) {
            // do nothing
        }
    }
}