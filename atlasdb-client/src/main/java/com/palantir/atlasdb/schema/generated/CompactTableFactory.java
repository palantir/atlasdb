package com.palantir.atlasdb.schema.generated;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.table.generation.Triggers;
import com.palantir.atlasdb.transaction.api.Transaction;
import java.util.List;
import javax.annotation.processing.Generated;

@Generated("com.palantir.atlasdb.table.description.render.TableFactoryRenderer")
public final class CompactTableFactory {
    private static final Namespace defaultNamespace = Namespace.create("compact", Namespace.UNCHECKED_NAME);

    private static final CompactTableFactory defaultTableFactory = of(defaultNamespace);

    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;

    private final Namespace namespace;

    private CompactTableFactory(
            List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        this.sharedTriggers = sharedTriggers;
        this.namespace = namespace;
    }

    public static CompactTableFactory of(
            List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        return new CompactTableFactory(sharedTriggers, namespace);
    }

    public static CompactTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new CompactTableFactory(sharedTriggers, defaultNamespace);
    }

    public static CompactTableFactory of(Namespace namespace) {
        return of(ImmutableList.of(), namespace);
    }

    public static CompactTableFactory of() {
        return defaultTableFactory;
    }

    public CompactMetadataTable getCompactMetadataTable(
            Transaction t, CompactMetadataTable.CompactMetadataTrigger... triggers) {
        return CompactMetadataTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers extends CompactMetadataTable.CompactMetadataTrigger {}

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putCompactMetadata(
                Multimap<
                                CompactMetadataTable.CompactMetadataRow,
                                ? extends CompactMetadataTable.CompactMetadataNamedColumnValue<?>>
                        newRows) {
            // do nothing
        }
    }
}
