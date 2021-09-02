package com.palantir.atlasdb.todo.generated;

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
public final class TodoSchemaTableFactory {
    private static final Namespace defaultNamespace = Namespace.create("default", Namespace.UNCHECKED_NAME);

    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;

    private final Namespace namespace;

    private TodoSchemaTableFactory(
            List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        this.sharedTriggers = sharedTriggers;
        this.namespace = namespace;
    }

    public static TodoSchemaTableFactory of(
            List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        return new TodoSchemaTableFactory(sharedTriggers, namespace);
    }

    public static TodoSchemaTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new TodoSchemaTableFactory(sharedTriggers, defaultNamespace);
    }

    public static TodoSchemaTableFactory of(Namespace namespace) {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), namespace);
    }

    public static TodoSchemaTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), defaultNamespace);
    }

    public LatestSnapshotTable getLatestSnapshotTable(
            Transaction t, LatestSnapshotTable.LatestSnapshotTrigger... triggers) {
        return LatestSnapshotTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public NamespacedTodoTable getNamespacedTodoTable(
            Transaction t, NamespacedTodoTable.NamespacedTodoTrigger... triggers) {
        return NamespacedTodoTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public SnapshotsStreamHashAidxTable getSnapshotsStreamHashAidxTable(
            Transaction t, SnapshotsStreamHashAidxTable.SnapshotsStreamHashAidxTrigger... triggers) {
        return SnapshotsStreamHashAidxTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public SnapshotsStreamIdxTable getSnapshotsStreamIdxTable(
            Transaction t, SnapshotsStreamIdxTable.SnapshotsStreamIdxTrigger... triggers) {
        return SnapshotsStreamIdxTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public SnapshotsStreamMetadataTable getSnapshotsStreamMetadataTable(
            Transaction t, SnapshotsStreamMetadataTable.SnapshotsStreamMetadataTrigger... triggers) {
        return SnapshotsStreamMetadataTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public SnapshotsStreamValueTable getSnapshotsStreamValueTable(
            Transaction t, SnapshotsStreamValueTable.SnapshotsStreamValueTrigger... triggers) {
        return SnapshotsStreamValueTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public TodoTable getTodoTable(Transaction t, TodoTable.TodoTrigger... triggers) {
        return TodoTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers
            extends LatestSnapshotTable.LatestSnapshotTrigger,
                    NamespacedTodoTable.NamespacedTodoTrigger,
                    SnapshotsStreamHashAidxTable.SnapshotsStreamHashAidxTrigger,
                    SnapshotsStreamIdxTable.SnapshotsStreamIdxTrigger,
                    SnapshotsStreamMetadataTable.SnapshotsStreamMetadataTrigger,
                    SnapshotsStreamValueTable.SnapshotsStreamValueTrigger,
                    TodoTable.TodoTrigger {}

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putLatestSnapshot(
                Multimap<
                                LatestSnapshotTable.LatestSnapshotRow,
                                ? extends LatestSnapshotTable.LatestSnapshotNamedColumnValue<?>>
                        newRows) {
            // do nothing
        }

        @Override
        public void putNamespacedTodo(
                Multimap<NamespacedTodoTable.NamespacedTodoRow, ? extends NamespacedTodoTable.NamespacedTodoColumnValue>
                        newRows) {
            // do nothing
        }

        @Override
        public void putSnapshotsStreamHashAidx(
                Multimap<
                                SnapshotsStreamHashAidxTable.SnapshotsStreamHashAidxRow,
                                ? extends SnapshotsStreamHashAidxTable.SnapshotsStreamHashAidxColumnValue>
                        newRows) {
            // do nothing
        }

        @Override
        public void putSnapshotsStreamIdx(
                Multimap<
                                SnapshotsStreamIdxTable.SnapshotsStreamIdxRow,
                                ? extends SnapshotsStreamIdxTable.SnapshotsStreamIdxColumnValue>
                        newRows) {
            // do nothing
        }

        @Override
        public void putSnapshotsStreamMetadata(
                Multimap<
                                SnapshotsStreamMetadataTable.SnapshotsStreamMetadataRow,
                                ? extends SnapshotsStreamMetadataTable.SnapshotsStreamMetadataNamedColumnValue<?>>
                        newRows) {
            // do nothing
        }

        @Override
        public void putSnapshotsStreamValue(
                Multimap<
                                SnapshotsStreamValueTable.SnapshotsStreamValueRow,
                                ? extends SnapshotsStreamValueTable.SnapshotsStreamValueNamedColumnValue<?>>
                        newRows) {
            // do nothing
        }

        @Override
        public void putTodo(Multimap<TodoTable.TodoRow, ? extends TodoTable.TodoNamedColumnValue<?>> newRows) {
            // do nothing
        }
    }
}
