package com.palantir.atlasdb.timelock.benchmarks.schema.generated;

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
public final class BenchmarksTableFactory {
    private static final Namespace defaultNamespace = Namespace.create("benchmarks", Namespace.UNCHECKED_NAME);

    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;

    private final Namespace namespace;

    private BenchmarksTableFactory(
            List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        this.sharedTriggers = sharedTriggers;
        this.namespace = namespace;
    }

    public static BenchmarksTableFactory of(
            List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        return new BenchmarksTableFactory(sharedTriggers, namespace);
    }

    public static BenchmarksTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new BenchmarksTableFactory(sharedTriggers, defaultNamespace);
    }

    public static BenchmarksTableFactory of(Namespace namespace) {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), namespace);
    }

    public static BenchmarksTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), defaultNamespace);
    }

    public BlobsTable getBlobsTable(Transaction t, BlobsTable.BlobsTrigger... triggers) {
        return BlobsTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public BlobsSerializableTable getBlobsSerializableTable(
            Transaction t, BlobsSerializableTable.BlobsSerializableTrigger... triggers) {
        return BlobsSerializableTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public KvDynamicColumnsTable getKvDynamicColumnsTable(
            Transaction t, KvDynamicColumnsTable.KvDynamicColumnsTrigger... triggers) {
        return KvDynamicColumnsTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public KvRowsTable getKvRowsTable(Transaction t, KvRowsTable.KvRowsTrigger... triggers) {
        return KvRowsTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public MetadataTable getMetadataTable(Transaction t, MetadataTable.MetadataTrigger... triggers) {
        return MetadataTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers
            extends BlobsTable.BlobsTrigger,
                    BlobsSerializableTable.BlobsSerializableTrigger,
                    KvDynamicColumnsTable.KvDynamicColumnsTrigger,
                    KvRowsTable.KvRowsTrigger,
                    MetadataTable.MetadataTrigger {}

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putBlobs(Multimap<BlobsTable.BlobsRow, ? extends BlobsTable.BlobsNamedColumnValue<?>> newRows) {
            // do nothing
        }

        @Override
        public void putBlobsSerializable(
                Multimap<
                                BlobsSerializableTable.BlobsSerializableRow,
                                ? extends BlobsSerializableTable.BlobsSerializableNamedColumnValue<?>>
                        newRows) {
            // do nothing
        }

        @Override
        public void putKvDynamicColumns(
                Multimap<
                                KvDynamicColumnsTable.KvDynamicColumnsRow,
                                ? extends KvDynamicColumnsTable.KvDynamicColumnsColumnValue>
                        newRows) {
            // do nothing
        }

        @Override
        public void putKvRows(
                Multimap<KvRowsTable.KvRowsRow, ? extends KvRowsTable.KvRowsNamedColumnValue<?>> newRows) {
            // do nothing
        }

        @Override
        public void putMetadata(
                Multimap<MetadataTable.MetadataRow, ? extends MetadataTable.MetadataNamedColumnValue<?>> newRows) {
            // do nothing
        }
    }
}
