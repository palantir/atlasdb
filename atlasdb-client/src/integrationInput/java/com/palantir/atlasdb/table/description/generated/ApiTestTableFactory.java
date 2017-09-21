package com.palantir.atlasdb.table.description.generated;

import java.util.List;

import javax.annotation.Generated;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.table.generation.Triggers;
import com.palantir.atlasdb.transaction.api.Transaction;

@Generated("com.palantir.atlasdb.table.description.render.TableFactoryRenderer")
public final class ApiTestTableFactory {
    private final static Namespace defaultNamespace = Namespace.create("default", Namespace.UNCHECKED_NAME);
    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;
    private final Namespace namespace;

    public static ApiTestTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        return new ApiTestTableFactory(sharedTriggers, namespace);
    }

    public static ApiTestTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new ApiTestTableFactory(sharedTriggers, defaultNamespace);
    }

    private ApiTestTableFactory(List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        this.sharedTriggers = sharedTriggers;
        this.namespace = namespace;
    }

    public static ApiTestTableFactory of(Namespace namespace) {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), namespace);
    }

    public static ApiTestTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), defaultNamespace);
    }

    public SchemaApiTestTable getSchemaApiTestTable(Transaction t, SchemaApiTestTable.SchemaApiTestTrigger... triggers) {
        return SchemaApiTestTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers extends
            SchemaApiTestTable.SchemaApiTestTrigger {
        /* empty */
    }

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putSchemaApiTest(Multimap<SchemaApiTestTable.SchemaApiTestRow, ? extends SchemaApiTestTable.SchemaApiTestNamedColumnValue<?>> newRows) {
            // do nothing
        }
    }
}