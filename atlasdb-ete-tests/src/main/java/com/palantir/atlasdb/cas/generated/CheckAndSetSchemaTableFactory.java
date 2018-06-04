package com.palantir.atlasdb.cas.generated;

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
public final class CheckAndSetSchemaTableFactory {
    private static final Namespace defaultNamespace = Namespace.create("default", Namespace.UNCHECKED_NAME);

    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;

    private final Namespace namespace;

    private CheckAndSetSchemaTableFactory(List<Function<? super Transaction, SharedTriggers>> sharedTriggers,
            Namespace namespace) {
        this.sharedTriggers = sharedTriggers;
        this.namespace = namespace;
    }

    public static CheckAndSetSchemaTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers,
            Namespace namespace) {
        return new CheckAndSetSchemaTableFactory(sharedTriggers, namespace);
    }

    public static CheckAndSetSchemaTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new CheckAndSetSchemaTableFactory(sharedTriggers, defaultNamespace);
    }

    public static CheckAndSetSchemaTableFactory of(Namespace namespace) {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), namespace);
    }

    public static CheckAndSetSchemaTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), defaultNamespace);
    }

    public CheckAndSetTable getCheckAndSetTable(Transaction t,
            CheckAndSetTable.CheckAndSetTrigger... triggers) {
        return CheckAndSetTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers extends CheckAndSetTable.CheckAndSetTrigger {
    }

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putCheckAndSet(Multimap<CheckAndSetTable.CheckAndSetRow, ? extends CheckAndSetTable.CheckAndSetNamedColumnValue<?>> newRows) {
            // do nothing
        }
    }
}
