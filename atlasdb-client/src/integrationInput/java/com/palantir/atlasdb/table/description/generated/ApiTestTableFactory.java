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
public final class ApiTestTableFactory {
    private static final Namespace defaultNamespace = Namespace.create("default", Namespace.UNCHECKED_NAME);

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
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), namespace);
    }

    public static ApiTestTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), defaultNamespace);
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

    public SchemaApiTestV2Table getSchemaApiTestV2Table(Transaction t) {
        return SchemaApiTestV2Table.of(t, namespace);
    }

    public interface SharedTriggers
            extends AllValueTypesTestTable.AllValueTypesTestTrigger,
                    HashComponentsTestTable.HashComponentsTestTrigger,
                    SchemaApiTestTable.SchemaApiTestTrigger {}

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putAllValueTypesTest(
                Multimap<
                                AllValueTypesTestTable.AllValueTypesTestRow,
                                ? extends AllValueTypesTestTable.AllValueTypesTestNamedColumnValue<?>>
_newRows) {
            // do nothing
        }

        @Override
        public void putHashComponentsTest(
                Multimap<
                                HashComponentsTestTable.HashComponentsTestRow,
                                ? extends HashComponentsTestTable.HashComponentsTestNamedColumnValue<?>>
_newRows) {
            // do nothing
        }

        @Override
        public void putSchemaApiTest(
                Multimap<
                                SchemaApiTestTable.SchemaApiTestRow,
                                ? extends SchemaApiTestTable.SchemaApiTestNamedColumnValue<?>>
_newRows) {
            // do nothing
        }
    }
}
