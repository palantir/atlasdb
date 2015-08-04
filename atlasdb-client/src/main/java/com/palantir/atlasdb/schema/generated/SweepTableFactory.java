/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.schema.generated;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.table.generation.Triggers;
import com.palantir.atlasdb.transaction.api.Transaction;

public class SweepTableFactory {
    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;

    public static SweepTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new SweepTableFactory(sharedTriggers);
    }

    private SweepTableFactory(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        this.sharedTriggers = sharedTriggers;
    }

    public static SweepTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of());
    }

    public SweepPriorityTable getSweepPriorityTable(Transaction t, SweepPriorityTable.SweepPriorityTrigger... triggers) {
        return SweepPriorityTable.of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public SweepProgressTable getSweepProgressTable(Transaction t, SweepProgressTable.SweepProgressTrigger... triggers) {
        return SweepProgressTable.of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers extends 
            SweepPriorityTable.SweepPriorityTrigger,
            SweepProgressTable.SweepProgressTrigger {
        /* empty */
    }

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putSweepPriority(Multimap<SweepPriorityTable.SweepPriorityRow, ? extends SweepPriorityTable.SweepPriorityNamedColumnValue<?>> newRows) {
            // do nothing
        }

        @Override
        public void putSweepProgress(Multimap<SweepProgressTable.SweepProgressRow, ? extends SweepProgressTable.SweepProgressNamedColumnValue<?>> newRows) {
            // do nothing
        }
    }
}
