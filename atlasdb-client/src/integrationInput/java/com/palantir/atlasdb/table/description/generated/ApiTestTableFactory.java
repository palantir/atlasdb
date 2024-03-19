package com.palantir.atlasdb.table.description.generated;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.table.generation.Triggers;
import com.palantir.atlasdb.transaction.api.Transaction;
import java.util.List;
import javax.annotation.processing.Generated;

@Generated("com.palantir.atlasdb.table.description.render.TableFactoryRenderer")
public final class ApiTestTableFactory {
    private static final Namespace defaultNamespace = Namespace.create("default", Namespace.UNCHECKED_NAME);

    private static final ApiTestTableFactory defaultTableFactory = of(defaultNamespace);

    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;

    private final Namespace namespace;

    private ApiTestTableFactory(
            List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        this.sharedTriggers = sharedTriggers;
        this.namespace = namespace;
    }

    public static ApiTestTableFactory of(
            List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        return new ApiTestTableFactory(sharedTriggers, namespace);
    }

    public static ApiTestTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new ApiTestTableFactory(sharedTriggers, defaultNamespace);
    }

    public static ApiTestTableFactory of(Namespace namespace) {
        return of(ImmutableList.of(), namespace);
    }

    public static ApiTestTableFactory of() {
        return defaultTableFactory;
    }

    public AllValueTypesTestTable getAllValueTypesTestTable(
            Transaction t, AllValueTypesTestTable.AllValueTypesTestTrigger... triggers) {
        return AllValueTypesTestTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public HashComponentsTestTable getHashComponentsTestTable(
            Transaction t, HashComponentsTestTable.HashComponentsTestTrigger... triggers) {
        return HashComponentsTestTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public SchemaApiTestTable getSchemaApiTestTable(
            Transaction t, SchemaApiTestTable.SchemaApiTestTrigger... triggers) {
        return SchemaApiTestTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
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

    public SchemaApiTestV2Table getSchemaApiTestV2Table(Transaction t) {
        return SchemaApiTestV2Table.of(t, namespace);
    }

    public interface SharedTriggers
            extends AllValueTypesTestTable.AllValueTypesTestTrigger,
                    HashComponentsTestTable.HashComponentsTestTrigger,
                    SchemaApiTestTable.SchemaApiTestTrigger,
                    StreamTestStreamHashAidxTable.StreamTestStreamHashAidxTrigger,
                    StreamTestStreamIdxTable.StreamTestStreamIdxTrigger,
                    StreamTestStreamMetadataTable.StreamTestStreamMetadataTrigger,
                    StreamTestStreamValueTable.StreamTestStreamValueTrigger {}

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putAllValueTypesTest(
                Multimap<
                                AllValueTypesTestTable.AllValueTypesTestRow,
                                ? extends AllValueTypesTestTable.AllValueTypesTestNamedColumnValue<?>>
                        newRows) {
            // do nothing
        }

        @Override
        public void putHashComponentsTest(
                Multimap<
                                HashComponentsTestTable.HashComponentsTestRow,
                                ? extends HashComponentsTestTable.HashComponentsTestNamedColumnValue<?>>
                        newRows) {
            // do nothing
        }

        @Override
        public void putSchemaApiTest(
                Multimap<
                                SchemaApiTestTable.SchemaApiTestRow,
                                ? extends SchemaApiTestTable.SchemaApiTestNamedColumnValue<?>>
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
    }
}
