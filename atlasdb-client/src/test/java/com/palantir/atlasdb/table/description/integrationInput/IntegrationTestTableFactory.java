/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.table.description.integrationInput;

import java.util.List;

import javax.annotation.Generated;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.table.generation.Triggers;
import com.palantir.atlasdb.transaction.api.Transaction;

@Generated("com.palantir.atlasdb.table.description.render.TableFactoryRenderer")
public final class IntegrationTestTableFactory {
    private final static Namespace defaultNamespace = Namespace.create("default", Namespace.UNCHECKED_NAME);
    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;
    private final Namespace namespace;

    public static IntegrationTestTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        return new IntegrationTestTableFactory(sharedTriggers, namespace);
    }

    public static IntegrationTestTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new IntegrationTestTableFactory(sharedTriggers, defaultNamespace);
    }

    private IntegrationTestTableFactory(List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        this.sharedTriggers = sharedTriggers;
        this.namespace = namespace;
    }

    public static IntegrationTestTableFactory of(Namespace namespace) {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), namespace);
    }

    public static IntegrationTestTableFactory of() {
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