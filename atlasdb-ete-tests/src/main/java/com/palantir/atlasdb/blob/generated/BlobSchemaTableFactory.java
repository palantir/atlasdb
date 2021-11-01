package com.palantir.atlasdb.blob.generated;

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
public final class BlobSchemaTableFactory {
    private static final Namespace defaultNamespace = Namespace.create("blob", Namespace.UNCHECKED_NAME);

    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;

    private final Namespace namespace;

    private BlobSchemaTableFactory(
            List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        this.sharedTriggers = sharedTriggers;
        this.namespace = namespace;
    }

    public static BlobSchemaTableFactory of(
            List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        return new BlobSchemaTableFactory(sharedTriggers, namespace);
    }

    public static BlobSchemaTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new BlobSchemaTableFactory(sharedTriggers, defaultNamespace);
    }

    public static BlobSchemaTableFactory of(Namespace namespace) {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), namespace);
    }

    public static BlobSchemaTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), defaultNamespace);
    }

    public AuditedDataTable getAuditedDataTable(Transaction t, AuditedDataTable.AuditedDataTrigger... triggers) {
        return AuditedDataTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public DataStreamHashAidxTable getDataStreamHashAidxTable(
            Transaction t, DataStreamHashAidxTable.DataStreamHashAidxTrigger... triggers) {
        return DataStreamHashAidxTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public DataStreamIdxTable getDataStreamIdxTable(
            Transaction t, DataStreamIdxTable.DataStreamIdxTrigger... triggers) {
        return DataStreamIdxTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public DataStreamMetadataTable getDataStreamMetadataTable(
            Transaction t, DataStreamMetadataTable.DataStreamMetadataTrigger... triggers) {
        return DataStreamMetadataTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public DataStreamValueTable getDataStreamValueTable(
            Transaction t, DataStreamValueTable.DataStreamValueTrigger... triggers) {
        return DataStreamValueTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public HotspottyDataStreamHashAidxTable getHotspottyDataStreamHashAidxTable(
            Transaction t, HotspottyDataStreamHashAidxTable.HotspottyDataStreamHashAidxTrigger... triggers) {
        return HotspottyDataStreamHashAidxTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public HotspottyDataStreamIdxTable getHotspottyDataStreamIdxTable(
            Transaction t, HotspottyDataStreamIdxTable.HotspottyDataStreamIdxTrigger... triggers) {
        return HotspottyDataStreamIdxTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public HotspottyDataStreamMetadataTable getHotspottyDataStreamMetadataTable(
            Transaction t, HotspottyDataStreamMetadataTable.HotspottyDataStreamMetadataTrigger... triggers) {
        return HotspottyDataStreamMetadataTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public HotspottyDataStreamValueTable getHotspottyDataStreamValueTable(
            Transaction t, HotspottyDataStreamValueTable.HotspottyDataStreamValueTrigger... triggers) {
        return HotspottyDataStreamValueTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers
            extends AuditedDataTable.AuditedDataTrigger,
                    DataStreamHashAidxTable.DataStreamHashAidxTrigger,
                    DataStreamIdxTable.DataStreamIdxTrigger,
                    DataStreamMetadataTable.DataStreamMetadataTrigger,
                    DataStreamValueTable.DataStreamValueTrigger,
                    HotspottyDataStreamHashAidxTable.HotspottyDataStreamHashAidxTrigger,
                    HotspottyDataStreamIdxTable.HotspottyDataStreamIdxTrigger,
                    HotspottyDataStreamMetadataTable.HotspottyDataStreamMetadataTrigger,
                    HotspottyDataStreamValueTable.HotspottyDataStreamValueTrigger {}

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putAuditedData(
                Multimap<AuditedDataTable.AuditedDataRow, ? extends AuditedDataTable.AuditedDataNamedColumnValue<?>>
_newRows) {
            // do nothing
        }

        @Override
        public void putDataStreamHashAidx(
                Multimap<
                                DataStreamHashAidxTable.DataStreamHashAidxRow,
                                ? extends DataStreamHashAidxTable.DataStreamHashAidxColumnValue>
_newRows) {
            // do nothing
        }

        @Override
        public void putDataStreamIdx(
                Multimap<DataStreamIdxTable.DataStreamIdxRow, ? extends DataStreamIdxTable.DataStreamIdxColumnValue>
_newRows) {
            // do nothing
        }

        @Override
        public void putDataStreamMetadata(
                Multimap<
                                DataStreamMetadataTable.DataStreamMetadataRow,
                                ? extends DataStreamMetadataTable.DataStreamMetadataNamedColumnValue<?>>
_newRows) {
            // do nothing
        }

        @Override
        public void putDataStreamValue(
                Multimap<
                                DataStreamValueTable.DataStreamValueRow,
                                ? extends DataStreamValueTable.DataStreamValueNamedColumnValue<?>>
_newRows) {
            // do nothing
        }

        @Override
        public void putHotspottyDataStreamHashAidx(
                Multimap<
                                HotspottyDataStreamHashAidxTable.HotspottyDataStreamHashAidxRow,
                                ? extends HotspottyDataStreamHashAidxTable.HotspottyDataStreamHashAidxColumnValue>
_newRows) {
            // do nothing
        }

        @Override
        public void putHotspottyDataStreamIdx(
                Multimap<
                                HotspottyDataStreamIdxTable.HotspottyDataStreamIdxRow,
                                ? extends HotspottyDataStreamIdxTable.HotspottyDataStreamIdxColumnValue>
_newRows) {
            // do nothing
        }

        @Override
        public void putHotspottyDataStreamMetadata(
                Multimap<
                                HotspottyDataStreamMetadataTable.HotspottyDataStreamMetadataRow,
                                ? extends
                                        HotspottyDataStreamMetadataTable.HotspottyDataStreamMetadataNamedColumnValue<?>>
_newRows) {
            // do nothing
        }

        @Override
        public void putHotspottyDataStreamValue(
                Multimap<
                                HotspottyDataStreamValueTable.HotspottyDataStreamValueRow,
                                ? extends HotspottyDataStreamValueTable.HotspottyDataStreamValueNamedColumnValue<?>>
_newRows) {
            // do nothing
        }
    }
}
