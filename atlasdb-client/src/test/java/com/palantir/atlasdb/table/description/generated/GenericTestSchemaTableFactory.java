package com.palantir.atlasdb.table.description.generated;

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
public final class GenericTestSchemaTableFactory {
    private static final Namespace defaultNamespace = Namespace.create("test", Namespace.UNCHECKED_NAME);

    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;

    private final Namespace namespace;

    private GenericTestSchemaTableFactory(
            List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        this.sharedTriggers = sharedTriggers;
        this.namespace = namespace;
    }

    public static GenericTestSchemaTableFactory of(
            List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        return new GenericTestSchemaTableFactory(sharedTriggers, namespace);
    }

    public static GenericTestSchemaTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new GenericTestSchemaTableFactory(sharedTriggers, defaultNamespace);
    }

    public static GenericTestSchemaTableFactory of(Namespace namespace) {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), namespace);
    }

    public static GenericTestSchemaTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), defaultNamespace);
    }

    public GenericRangeScanTestTable getGenericRangeScanTestTable(
            Transaction t, GenericRangeScanTestTable.GenericRangeScanTestTrigger... triggers) {
        return GenericRangeScanTestTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public RangeScanTestTable getRangeScanTestTable(
            Transaction t, RangeScanTestTable.RangeScanTestTrigger... triggers) {
        return RangeScanTestTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers
            extends GenericRangeScanTestTable.GenericRangeScanTestTrigger, RangeScanTestTable.RangeScanTestTrigger {}

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putGenericRangeScanTest(
                Multimap<
                                GenericRangeScanTestTable.GenericRangeScanTestRow,
                                ? extends GenericRangeScanTestTable.GenericRangeScanTestColumnValue>
_newRows) {
            // do nothing
        }

        @Override
        public void putRangeScanTest(
                Multimap<
                                RangeScanTestTable.RangeScanTestRow,
                                ? extends RangeScanTestTable.RangeScanTestNamedColumnValue<?>>
_newRows) {
            // do nothing
        }
    }
}
