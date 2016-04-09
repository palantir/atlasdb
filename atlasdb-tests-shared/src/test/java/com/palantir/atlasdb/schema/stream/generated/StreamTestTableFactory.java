package com.palantir.atlasdb.schema.stream.generated;

import java.util.List;

import javax.annotation.Generated;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.table.generation.Triggers;
import com.palantir.atlasdb.transaction.api.Transaction;

@Generated("com.palantir.atlasdb.table.description.render.TableFactoryRenderer")
public final class StreamTestTableFactory {
    private final static Namespace defaultNamespace = Namespace.create("default", Namespace.UNCHECKED_NAME);
    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;
    private final Namespace namespace;

    public static StreamTestTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        return new StreamTestTableFactory(sharedTriggers, namespace);
    }

    public static StreamTestTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new StreamTestTableFactory(sharedTriggers, defaultNamespace);
    }

    private StreamTestTableFactory(List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        this.sharedTriggers = sharedTriggers;
        this.namespace = namespace;
    }

    public static StreamTestTableFactory of(Namespace namespace) {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), namespace);
    }

    public static StreamTestTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), defaultNamespace);
    }

    public StreamTestStreamHashAidxTable getStreamTestStreamHashAidxTable(Transaction t, StreamTestStreamHashAidxTable.StreamTestStreamHashAidxTrigger... triggers) {
        return StreamTestStreamHashAidxTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTestStreamIdxTable getStreamTestStreamIdxTable(Transaction t, StreamTestStreamIdxTable.StreamTestStreamIdxTrigger... triggers) {
        return StreamTestStreamIdxTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTestStreamMetadataTable getStreamTestStreamMetadataTable(Transaction t, StreamTestStreamMetadataTable.StreamTestStreamMetadataTrigger... triggers) {
        return StreamTestStreamMetadataTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTestStreamValueTable getStreamTestStreamValueTable(Transaction t, StreamTestStreamValueTable.StreamTestStreamValueTrigger... triggers) {
        return StreamTestStreamValueTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTestWithHashStreamHashAidxTable getStreamTestWithHashStreamHashAidxTable(Transaction t, StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxTrigger... triggers) {
        return StreamTestWithHashStreamHashAidxTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTestWithHashStreamIdxTable getStreamTestWithHashStreamIdxTable(Transaction t, StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxTrigger... triggers) {
        return StreamTestWithHashStreamIdxTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTestWithHashStreamMetadataTable getStreamTestWithHashStreamMetadataTable(Transaction t, StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataTrigger... triggers) {
        return StreamTestWithHashStreamMetadataTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTestWithHashStreamValueTable getStreamTestWithHashStreamValueTable(Transaction t, StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueTrigger... triggers) {
        return StreamTestWithHashStreamValueTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers extends
            StreamTestStreamHashAidxTable.StreamTestStreamHashAidxTrigger,
            StreamTestStreamIdxTable.StreamTestStreamIdxTrigger,
            StreamTestStreamMetadataTable.StreamTestStreamMetadataTrigger,
            StreamTestStreamValueTable.StreamTestStreamValueTrigger,
            StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxTrigger,
            StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxTrigger,
            StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataTrigger,
            StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueTrigger {
        /* empty */
    }

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putStreamTestStreamHashAidx(Multimap<StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow, ? extends StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumnValue> newRows) {
            // do nothing
        }

        @Override
        public void putStreamTestStreamIdx(Multimap<StreamTestStreamIdxTable.StreamTestStreamIdxRow, ? extends StreamTestStreamIdxTable.StreamTestStreamIdxColumnValue> newRows) {
            // do nothing
        }

        @Override
        public void putStreamTestStreamMetadata(Multimap<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, ? extends StreamTestStreamMetadataTable.StreamTestStreamMetadataNamedColumnValue<?>> newRows) {
            // do nothing
        }

        @Override
        public void putStreamTestStreamValue(Multimap<StreamTestStreamValueTable.StreamTestStreamValueRow, ? extends StreamTestStreamValueTable.StreamTestStreamValueNamedColumnValue<?>> newRows) {
            // do nothing
        }

        @Override
        public void putStreamTestWithHashStreamHashAidx(Multimap<StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxRow, ? extends StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxColumnValue> newRows) {
            // do nothing
        }

        @Override
        public void putStreamTestWithHashStreamIdx(Multimap<StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow, ? extends StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxColumnValue> newRows) {
            // do nothing
        }

        @Override
        public void putStreamTestWithHashStreamMetadata(Multimap<StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow, ? extends StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataNamedColumnValue<?>> newRows) {
            // do nothing
        }

        @Override
        public void putStreamTestWithHashStreamValue(Multimap<StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueRow, ? extends StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueNamedColumnValue<?>> newRows) {
            // do nothing
        }
    }
}