package com.palantir.atlasdb.performance.schema.generated;

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
public final class StreamTestTableFactory {
    private static final Namespace defaultNamespace = Namespace.create("default", Namespace.UNCHECKED_NAME);

    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;

    private final Namespace namespace;

    private StreamTestTableFactory(
            List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        this.sharedTriggers = sharedTriggers;
        this.namespace = namespace;
    }

    public static StreamTestTableFactory of(
            List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        return new StreamTestTableFactory(sharedTriggers, namespace);
    }

    public static StreamTestTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new StreamTestTableFactory(sharedTriggers, defaultNamespace);
    }

    public static StreamTestTableFactory of(Namespace namespace) {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), namespace);
    }

    public static StreamTestTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), defaultNamespace);
    }

    public KeyValueTable getKeyValueTable(Transaction t, KeyValueTable.KeyValueTrigger... triggers) {
        return KeyValueTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public ValueStreamHashAidxTable getValueStreamHashAidxTable(
            Transaction t, ValueStreamHashAidxTable.ValueStreamHashAidxTrigger... triggers) {
        return ValueStreamHashAidxTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public ValueStreamIdxTable getValueStreamIdxTable(
            Transaction t, ValueStreamIdxTable.ValueStreamIdxTrigger... triggers) {
        return ValueStreamIdxTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public ValueStreamMetadataTable getValueStreamMetadataTable(
            Transaction t, ValueStreamMetadataTable.ValueStreamMetadataTrigger... triggers) {
        return ValueStreamMetadataTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public ValueStreamValueTable getValueStreamValueTable(
            Transaction t, ValueStreamValueTable.ValueStreamValueTrigger... triggers) {
        return ValueStreamValueTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers
            extends KeyValueTable.KeyValueTrigger,
                    ValueStreamHashAidxTable.ValueStreamHashAidxTrigger,
                    ValueStreamIdxTable.ValueStreamIdxTrigger,
                    ValueStreamMetadataTable.ValueStreamMetadataTrigger,
                    ValueStreamValueTable.ValueStreamValueTrigger {}

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putKeyValue(
                Multimap<KeyValueTable.KeyValueRow, ? extends KeyValueTable.KeyValueNamedColumnValue<?>> newRows) {
            // do nothing
        }

        @Override
        public void putValueStreamHashAidx(
                Multimap<
                                ValueStreamHashAidxTable.ValueStreamHashAidxRow,
                                ? extends ValueStreamHashAidxTable.ValueStreamHashAidxColumnValue>
                        newRows) {
            // do nothing
        }

        @Override
        public void putValueStreamIdx(
                Multimap<ValueStreamIdxTable.ValueStreamIdxRow, ? extends ValueStreamIdxTable.ValueStreamIdxColumnValue>
                        newRows) {
            // do nothing
        }

        @Override
        public void putValueStreamMetadata(
                Multimap<
                                ValueStreamMetadataTable.ValueStreamMetadataRow,
                                ? extends ValueStreamMetadataTable.ValueStreamMetadataNamedColumnValue<?>>
                        newRows) {
            // do nothing
        }

        @Override
        public void putValueStreamValue(
                Multimap<
                                ValueStreamValueTable.ValueStreamValueRow,
                                ? extends ValueStreamValueTable.ValueStreamValueNamedColumnValue<?>>
                        newRows) {
            // do nothing
        }
    }
}
