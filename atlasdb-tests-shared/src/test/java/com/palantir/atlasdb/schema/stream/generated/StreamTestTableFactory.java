package com.palantir.atlasdb.schema.stream.generated;

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

    public StreamTestMaxMemStreamHashAidxTable getStreamTestMaxMemStreamHashAidxTable(
            Transaction t, StreamTestMaxMemStreamHashAidxTable.StreamTestMaxMemStreamHashAidxTrigger... triggers) {
        return StreamTestMaxMemStreamHashAidxTable.of(
                t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTestMaxMemStreamIdxTable getStreamTestMaxMemStreamIdxTable(
            Transaction t, StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxTrigger... triggers) {
        return StreamTestMaxMemStreamIdxTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTestMaxMemStreamMetadataTable getStreamTestMaxMemStreamMetadataTable(
            Transaction t, StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataTrigger... triggers) {
        return StreamTestMaxMemStreamMetadataTable.of(
                t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTestMaxMemStreamValueTable getStreamTestMaxMemStreamValueTable(
            Transaction t, StreamTestMaxMemStreamValueTable.StreamTestMaxMemStreamValueTrigger... triggers) {
        return StreamTestMaxMemStreamValueTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTestStreamHashAidxTable getStreamTestStreamHashAidxTable(
            Transaction t, StreamTestStreamHashAidxTable.StreamTestStreamHashAidxTrigger... triggers) {
        return StreamTestStreamHashAidxTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTestStreamIdxTable getStreamTestStreamIdxTable(
            Transaction t, StreamTestStreamIdxTable.StreamTestStreamIdxTrigger... triggers) {
        return StreamTestStreamIdxTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTestStreamMetadataTable getStreamTestStreamMetadataTable(
            Transaction t, StreamTestStreamMetadataTable.StreamTestStreamMetadataTrigger... triggers) {
        return StreamTestStreamMetadataTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTestStreamValueTable getStreamTestStreamValueTable(
            Transaction t, StreamTestStreamValueTable.StreamTestStreamValueTrigger... triggers) {
        return StreamTestStreamValueTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTestWithHashStreamHashAidxTable getStreamTestWithHashStreamHashAidxTable(
            Transaction t, StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxTrigger... triggers) {
        return StreamTestWithHashStreamHashAidxTable.of(
                t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTestWithHashStreamIdxTable getStreamTestWithHashStreamIdxTable(
            Transaction t, StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxTrigger... triggers) {
        return StreamTestWithHashStreamIdxTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTestWithHashStreamMetadataTable getStreamTestWithHashStreamMetadataTable(
            Transaction t, StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataTrigger... triggers) {
        return StreamTestWithHashStreamMetadataTable.of(
                t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTestWithHashStreamValueTable getStreamTestWithHashStreamValueTable(
            Transaction t, StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueTrigger... triggers) {
        return StreamTestWithHashStreamValueTable.of(
                t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public TestHashComponentsStreamHashAidxTable getTestHashComponentsStreamHashAidxTable(
            Transaction t, TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxTrigger... triggers) {
        return TestHashComponentsStreamHashAidxTable.of(
                t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public TestHashComponentsStreamIdxTable getTestHashComponentsStreamIdxTable(
            Transaction t, TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxTrigger... triggers) {
        return TestHashComponentsStreamIdxTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public TestHashComponentsStreamMetadataTable getTestHashComponentsStreamMetadataTable(
            Transaction t, TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataTrigger... triggers) {
        return TestHashComponentsStreamMetadataTable.of(
                t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public TestHashComponentsStreamValueTable getTestHashComponentsStreamValueTable(
            Transaction t, TestHashComponentsStreamValueTable.TestHashComponentsStreamValueTrigger... triggers) {
        return TestHashComponentsStreamValueTable.of(
                t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers
            extends KeyValueTable.KeyValueTrigger,
                    StreamTestMaxMemStreamHashAidxTable.StreamTestMaxMemStreamHashAidxTrigger,
                    StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxTrigger,
                    StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataTrigger,
                    StreamTestMaxMemStreamValueTable.StreamTestMaxMemStreamValueTrigger,
                    StreamTestStreamHashAidxTable.StreamTestStreamHashAidxTrigger,
                    StreamTestStreamIdxTable.StreamTestStreamIdxTrigger,
                    StreamTestStreamMetadataTable.StreamTestStreamMetadataTrigger,
                    StreamTestStreamValueTable.StreamTestStreamValueTrigger,
                    StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxTrigger,
                    StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxTrigger,
                    StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataTrigger,
                    StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueTrigger,
                    TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxTrigger,
                    TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxTrigger,
                    TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataTrigger,
                    TestHashComponentsStreamValueTable.TestHashComponentsStreamValueTrigger {}

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putKeyValue(
                Multimap<KeyValueTable.KeyValueRow, ? extends KeyValueTable.KeyValueNamedColumnValue<?>> newRows) {
            // do nothing
        }

        @Override
        public void putStreamTestMaxMemStreamHashAidx(
                Multimap<
                                StreamTestMaxMemStreamHashAidxTable.StreamTestMaxMemStreamHashAidxRow,
                                ? extends StreamTestMaxMemStreamHashAidxTable.StreamTestMaxMemStreamHashAidxColumnValue>
                        newRows) {
            // do nothing
        }

        @Override
        public void putStreamTestMaxMemStreamIdx(
                Multimap<
                                StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxRow,
                                ? extends StreamTestMaxMemStreamIdxTable.StreamTestMaxMemStreamIdxColumnValue>
                        newRows) {
            // do nothing
        }

        @Override
        public void putStreamTestMaxMemStreamMetadata(
                Multimap<
                                StreamTestMaxMemStreamMetadataTable.StreamTestMaxMemStreamMetadataRow,
                                ? extends
                                        StreamTestMaxMemStreamMetadataTable
                                                        .StreamTestMaxMemStreamMetadataNamedColumnValue<
                                                ?>>
                        newRows) {
            // do nothing
        }

        @Override
        public void putStreamTestMaxMemStreamValue(
                Multimap<
                                StreamTestMaxMemStreamValueTable.StreamTestMaxMemStreamValueRow,
                                ? extends
                                        StreamTestMaxMemStreamValueTable.StreamTestMaxMemStreamValueNamedColumnValue<?>>
                        newRows) {
            // do nothing
        }

        @Override
        public void putStreamTestStreamHashAidx(
                Multimap<
                                StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow,
                                ? extends StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumnValue>
                        newRows) {
            // do nothing
        }

        @Override
        public void putStreamTestStreamIdx(
                Multimap<
                                StreamTestStreamIdxTable.StreamTestStreamIdxRow,
                                ? extends StreamTestStreamIdxTable.StreamTestStreamIdxColumnValue>
                        newRows) {
            // do nothing
        }

        @Override
        public void putStreamTestStreamMetadata(
                Multimap<
                                StreamTestStreamMetadataTable.StreamTestStreamMetadataRow,
                                ? extends StreamTestStreamMetadataTable.StreamTestStreamMetadataNamedColumnValue<?>>
                        newRows) {
            // do nothing
        }

        @Override
        public void putStreamTestStreamValue(
                Multimap<
                                StreamTestStreamValueTable.StreamTestStreamValueRow,
                                ? extends StreamTestStreamValueTable.StreamTestStreamValueNamedColumnValue<?>>
                        newRows) {
            // do nothing
        }

        @Override
        public void putStreamTestWithHashStreamHashAidx(
                Multimap<
                                StreamTestWithHashStreamHashAidxTable.StreamTestWithHashStreamHashAidxRow,
                                ? extends
                                        StreamTestWithHashStreamHashAidxTable
                                                .StreamTestWithHashStreamHashAidxColumnValue>
                        newRows) {
            // do nothing
        }

        @Override
        public void putStreamTestWithHashStreamIdx(
                Multimap<
                                StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxRow,
                                ? extends StreamTestWithHashStreamIdxTable.StreamTestWithHashStreamIdxColumnValue>
                        newRows) {
            // do nothing
        }

        @Override
        public void putStreamTestWithHashStreamMetadata(
                Multimap<
                                StreamTestWithHashStreamMetadataTable.StreamTestWithHashStreamMetadataRow,
                                ? extends
                                        StreamTestWithHashStreamMetadataTable
                                                        .StreamTestWithHashStreamMetadataNamedColumnValue<
                                                ?>>
                        newRows) {
            // do nothing
        }

        @Override
        public void putStreamTestWithHashStreamValue(
                Multimap<
                                StreamTestWithHashStreamValueTable.StreamTestWithHashStreamValueRow,
                                ? extends
                                        StreamTestWithHashStreamValueTable
                                                        .StreamTestWithHashStreamValueNamedColumnValue<
                                                ?>>
                        newRows) {
            // do nothing
        }

        @Override
        public void putTestHashComponentsStreamHashAidx(
                Multimap<
                                TestHashComponentsStreamHashAidxTable.TestHashComponentsStreamHashAidxRow,
                                ? extends
                                        TestHashComponentsStreamHashAidxTable
                                                .TestHashComponentsStreamHashAidxColumnValue>
                        newRows) {
            // do nothing
        }

        @Override
        public void putTestHashComponentsStreamIdx(
                Multimap<
                                TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxRow,
                                ? extends TestHashComponentsStreamIdxTable.TestHashComponentsStreamIdxColumnValue>
                        newRows) {
            // do nothing
        }

        @Override
        public void putTestHashComponentsStreamMetadata(
                Multimap<
                                TestHashComponentsStreamMetadataTable.TestHashComponentsStreamMetadataRow,
                                ? extends
                                        TestHashComponentsStreamMetadataTable
                                                        .TestHashComponentsStreamMetadataNamedColumnValue<
                                                ?>>
                        newRows) {
            // do nothing
        }

        @Override
        public void putTestHashComponentsStreamValue(
                Multimap<
                                TestHashComponentsStreamValueTable.TestHashComponentsStreamValueRow,
                                ? extends
                                        TestHashComponentsStreamValueTable
                                                        .TestHashComponentsStreamValueNamedColumnValue<
                                                ?>>
                        newRows) {
            // do nothing
        }
    }
}
