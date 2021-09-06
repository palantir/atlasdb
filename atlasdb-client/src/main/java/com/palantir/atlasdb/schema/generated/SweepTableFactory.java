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
public final class SweepTableFactory {
    private static final Namespace defaultNamespace = Namespace.create("sweep", Namespace.UNCHECKED_NAME);

    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;

    private final Namespace namespace;

    private SweepTableFactory(List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        this.sharedTriggers = sharedTriggers;
        this.namespace = namespace;
    }

    public static SweepTableFactory of(
            List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        return new SweepTableFactory(sharedTriggers, namespace);
    }

    public static SweepTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new SweepTableFactory(sharedTriggers, defaultNamespace);
    }

    public static SweepTableFactory of(Namespace namespace) {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), namespace);
    }

    public static SweepTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), defaultNamespace);
    }

    public SweepPriorityTable getSweepPriorityTable(
            Transaction t, SweepPriorityTable.SweepPriorityTrigger... triggers) {
        return SweepPriorityTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers extends SweepPriorityTable.SweepPriorityTrigger {}

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putSweepPriority(
                Multimap<
                                SweepPriorityTable.SweepPriorityRow,
                                ? extends SweepPriorityTable.SweepPriorityNamedColumnValue<?>>
                        newRows) {
            // do nothing
        }
    }
}
