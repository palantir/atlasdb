package com.palantir.atlasdb.schema.generated;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.table.generation.Triggers;
import com.palantir.atlasdb.transaction.api.Transaction;

public class UpgradeTableFactory {
    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;

    public static UpgradeTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new UpgradeTableFactory(sharedTriggers);
    }

    private UpgradeTableFactory(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        this.sharedTriggers = sharedTriggers;
    }

    public static UpgradeTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of());
    }

    public UpgTaskMetadataTable getUpgTaskMetadataTable(Transaction t, UpgTaskMetadataTable.UpgTaskMetadataTrigger... triggers) {
        return UpgTaskMetadataTable.of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public UpgradeMetadataTable getUpgradeMetadataTable(Transaction t, UpgradeMetadataTable.UpgradeMetadataTrigger... triggers) {
        return UpgradeMetadataTable.of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));
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