package com.palantir.atlasdb.schema.generated;

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
public final class TargetedSweepTableFactory {
    private static final Namespace defaultNamespace = Namespace.create("sweep", Namespace.UNCHECKED_NAME);

    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;

    private final Namespace namespace;

    private TargetedSweepTableFactory(
            List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        this.sharedTriggers = sharedTriggers;
        this.namespace = namespace;
    }

    public static TargetedSweepTableFactory of(
            List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        return new TargetedSweepTableFactory(sharedTriggers, namespace);
    }

    public static TargetedSweepTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new TargetedSweepTableFactory(sharedTriggers, defaultNamespace);
    }

    public static TargetedSweepTableFactory of(Namespace namespace) {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), namespace);
    }

    public static TargetedSweepTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), defaultNamespace);
    }

    public SweepIdToNameTable getSweepIdToNameTable(
            Transaction t, SweepIdToNameTable.SweepIdToNameTrigger... triggers) {
        return SweepIdToNameTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public SweepNameToIdTable getSweepNameToIdTable(
            Transaction t, SweepNameToIdTable.SweepNameToIdTrigger... triggers) {
        return SweepNameToIdTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public SweepShardProgressTable getSweepShardProgressTable(
            Transaction t, SweepShardProgressTable.SweepShardProgressTrigger... triggers) {
        return SweepShardProgressTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public SweepableCellsTable getSweepableCellsTable(
            Transaction t, SweepableCellsTable.SweepableCellsTrigger... triggers) {
        return SweepableCellsTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public SweepableTimestampsTable getSweepableTimestampsTable(
            Transaction t, SweepableTimestampsTable.SweepableTimestampsTrigger... triggers) {
        return SweepableTimestampsTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public TableClearsTable getTableClearsTable(Transaction t, TableClearsTable.TableClearsTrigger... triggers) {
        return TableClearsTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers
            extends SweepIdToNameTable.SweepIdToNameTrigger,
                    SweepNameToIdTable.SweepNameToIdTrigger,
                    SweepShardProgressTable.SweepShardProgressTrigger,
                    SweepableCellsTable.SweepableCellsTrigger,
                    SweepableTimestampsTable.SweepableTimestampsTrigger,
                    TableClearsTable.TableClearsTrigger {}

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putSweepIdToName(
                Multimap<SweepIdToNameTable.SweepIdToNameRow, ? extends SweepIdToNameTable.SweepIdToNameColumnValue>
_newRows) {
            // do nothing
        }

        @Override
        public void putSweepNameToId(
                Multimap<
                                SweepNameToIdTable.SweepNameToIdRow,
                                ? extends SweepNameToIdTable.SweepNameToIdNamedColumnValue<?>>
_newRows) {
            // do nothing
        }

        @Override
        public void putSweepShardProgress(
                Multimap<
                                SweepShardProgressTable.SweepShardProgressRow,
                                ? extends SweepShardProgressTable.SweepShardProgressNamedColumnValue<?>>
_newRows) {
            // do nothing
        }

        @Override
        public void putSweepableCells(
                Multimap<SweepableCellsTable.SweepableCellsRow, ? extends SweepableCellsTable.SweepableCellsColumnValue>
_newRows) {
            // do nothing
        }

        @Override
        public void putSweepableTimestamps(
                Multimap<
                                SweepableTimestampsTable.SweepableTimestampsRow,
                                ? extends SweepableTimestampsTable.SweepableTimestampsColumnValue>
_newRows) {
            // do nothing
        }

        @Override
        public void putTableClears(
                Multimap<TableClearsTable.TableClearsRow, ? extends TableClearsTable.TableClearsNamedColumnValue<?>>
_newRows) {
            // do nothing
        }
    }
}
