package com.palantir.atlasdb.cas.generated;

import java.util.List;

import javax.annotation.Generated;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.table.generation.Triggers;
import com.palantir.atlasdb.transaction.api.Transaction;

@Generated("com.palantir.atlasdb.table.description.render.TableFactoryRenderer")
public final class CasSchemaTableFactory {
    private final static Namespace defaultNamespace = Namespace.create("default", Namespace.UNCHECKED_NAME);
    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;
    private final Namespace namespace;

    public static CasSchemaTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        return new CasSchemaTableFactory(sharedTriggers, namespace);
    }

    public static CasSchemaTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new CasSchemaTableFactory(sharedTriggers, defaultNamespace);
    }

    private CasSchemaTableFactory(List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        this.sharedTriggers = sharedTriggers;
        this.namespace = namespace;
    }

    public static CasSchemaTableFactory of(Namespace namespace) {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), namespace);
    }

    public static CasSchemaTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), defaultNamespace);
    }

    public CasTable getCasTable(Transaction t, CasTable.CasTrigger... triggers) {
        return CasTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers extends
            CasTable.CasTrigger {
        /* empty */
    }

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putCas(Multimap<CasTable.CasRow, ? extends CasTable.CasNamedColumnValue<?>> newRows) {
            // do nothing
        }
    }
}