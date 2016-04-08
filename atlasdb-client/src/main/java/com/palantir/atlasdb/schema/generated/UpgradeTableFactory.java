package com.palantir.atlasdb.schema.generated;

import java.util.List;

import javax.annotation.Generated;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.schema.Namespace;
import com.palantir.atlasdb.table.generation.Triggers;
import com.palantir.atlasdb.transaction.api.Transaction;

@Generated("com.palantir.atlasdb.table.description.render.TableFactoryRenderer")
public final class UpgradeTableFactory {
    private final static Namespace defaultNamespace = Namespace.create("upgrade", Namespace.UNCHECKED_NAME);
    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;
    private final Namespace namespace;

    public static UpgradeTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        return new UpgradeTableFactory(sharedTriggers, namespace);
    }

    public static UpgradeTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new UpgradeTableFactory(sharedTriggers, defaultNamespace);
    }

    private UpgradeTableFactory(List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        this.sharedTriggers = sharedTriggers;
        this.namespace = namespace;
    }

    public static UpgradeTableFactory of(Namespace namespace) {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), namespace);
    }

    public static UpgradeTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), defaultNamespace);
    }

    public UpgTaskMetadataTable getUpgTaskMetadataTable(Transaction t, UpgTaskMetadataTable.UpgTaskMetadataTrigger... triggers) {
        return UpgTaskMetadataTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public UpgradeMetadataTable getUpgradeMetadataTable(Transaction t, UpgradeMetadataTable.UpgradeMetadataTrigger... triggers) {
        return UpgradeMetadataTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers extends
            UpgTaskMetadataTable.UpgTaskMetadataTrigger,
            UpgradeMetadataTable.UpgradeMetadataTrigger {
        /* empty */
    }

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putUpgTaskMetadata(Multimap<UpgTaskMetadataTable.UpgTaskMetadataRow, ? extends UpgTaskMetadataTable.UpgTaskMetadataNamedColumnValue<?>> newRows) {
            // do nothing
        }

        @Override
        public void putUpgradeMetadata(Multimap<UpgradeMetadataTable.UpgradeMetadataRow, ? extends UpgradeMetadataTable.UpgradeMetadataNamedColumnValue<?>> newRows) {
            // do nothing
        }
    }
}