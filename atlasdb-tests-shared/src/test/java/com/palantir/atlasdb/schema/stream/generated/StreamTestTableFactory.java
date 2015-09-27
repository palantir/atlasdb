package com.palantir.atlasdb.schema.stream.generated;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.schema.Namespace;
import com.palantir.atlasdb.table.generation.Triggers;
import com.palantir.atlasdb.transaction.api.Transaction;

public class StreamTestTableFactory {
    private final static Namespace defaultNamespace = Namespace.create("default");
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

    public StreamTest2StreamHashAidxTable getStreamTest2StreamHashAidxTable(Transaction t, StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxTrigger... triggers) {
        return StreamTest2StreamHashAidxTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTest2StreamIdxTable getStreamTest2StreamIdxTable(Transaction t, StreamTest2StreamIdxTable.StreamTest2StreamIdxTrigger... triggers) {
        return StreamTest2StreamIdxTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTest2StreamMetadataTable getStreamTest2StreamMetadataTable(Transaction t, StreamTest2StreamMetadataTable.StreamTest2StreamMetadataTrigger... triggers) {
        return StreamTest2StreamMetadataTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTest2StreamValueTable getStreamTest2StreamValueTable(Transaction t, StreamTest2StreamValueTable.StreamTest2StreamValueTrigger... triggers) {
        return StreamTest2StreamValueTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
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

    public interface SharedTriggers extends
            StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxTrigger,
            StreamTest2StreamIdxTable.StreamTest2StreamIdxTrigger,
            StreamTest2StreamMetadataTable.StreamTest2StreamMetadataTrigger,
            StreamTest2StreamValueTable.StreamTest2StreamValueTrigger,
            StreamTestStreamHashAidxTable.StreamTestStreamHashAidxTrigger,
            StreamTestStreamIdxTable.StreamTestStreamIdxTrigger,
            StreamTestStreamMetadataTable.StreamTestStreamMetadataTrigger,
            StreamTestStreamValueTable.StreamTestStreamValueTrigger {
        /* empty */
    }

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putStreamTest2StreamHashAidx(Multimap<StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxRow, ? extends StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxColumnValue> newRows) {
            // do nothing
        }

        @Override
        public void putStreamTest2StreamIdx(Multimap<StreamTest2StreamIdxTable.StreamTest2StreamIdxRow, ? extends StreamTest2StreamIdxTable.StreamTest2StreamIdxColumnValue> newRows) {
            // do nothing
        }

        @Override
        public void putStreamTest2StreamMetadata(Multimap<StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRow, ? extends StreamTest2StreamMetadataTable.StreamTest2StreamMetadataNamedColumnValue<?>> newRows) {
            // do nothing
        }

        @Override
        public void putStreamTest2StreamValue(Multimap<StreamTest2StreamValueTable.StreamTest2StreamValueRow, ? extends StreamTest2StreamValueTable.StreamTest2StreamValueNamedColumnValue<?>> newRows) {
            // do nothing
        }

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
    }
}