// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.schema.indexing.generated;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.table.generation.Triggers;
import com.palantir.atlasdb.transaction.api.Transaction;

public class IndexTestTableFactory {
    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;

    public static IndexTestTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new IndexTestTableFactory(sharedTriggers);
    }

    private IndexTestTableFactory(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        this.sharedTriggers = sharedTriggers;
    }

    public static IndexTestTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of());
    }

    public DataTable getDataTable(Transaction t, DataTable.DataTrigger... triggers) {
        return DataTable.of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public TwoColumnsTable getTwoColumnsTable(Transaction t, TwoColumnsTable.TwoColumnsTrigger... triggers) {
        return TwoColumnsTable.of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers extends
            DataTable.DataTrigger,
            TwoColumnsTable.TwoColumnsTrigger {
        /* empty */
    }

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putData(Multimap<DataTable.DataRow, ? extends DataTable.DataNamedColumnValue<?>> newRows) {
            // do nothing
        }

        @Override
        public void putTwoColumns(Multimap<TwoColumnsTable.TwoColumnsRow, ? extends TwoColumnsTable.TwoColumnsNamedColumnValue<?>> newRows) {
            // do nothing
        }
    }
}