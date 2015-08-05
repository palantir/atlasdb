/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.schema.generated;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.table.generation.Triggers;
import com.palantir.atlasdb.transaction.api.Transaction;

public class UpgradeTableFactory {
    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;

    public static UpgradeTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new UpgradeTableFactory(sharedTriggers);
    }

    private UpgradeTableFactory(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        this.sharedTriggers = sharedTriggers;
    }

    public static UpgradeTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of());
    }

    public UpgTaskMetadataTable getUpgTaskMetadataTable(Transaction t, UpgTaskMetadataTable.UpgTaskMetadataTrigger... triggers) {
        return UpgTaskMetadataTable.of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public UpgradeMetadataTable getUpgradeMetadataTable(Transaction t, UpgradeMetadataTable.UpgradeMetadataTrigger... triggers) {
        return UpgradeMetadataTable.of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers extends
            UpgTaskMetadataTable.UpgTaskMetadataTrigger,
            UpgradeMetadataTable.UpgradeMetadataTrigger {
        /* empty */
    }

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putUpgTaskMetadata(Multimap<UpgTaskMetadataTable.UpgTaskMetadataRow, ? extends UpgTaskMetadataTable.UpgTaskMetadataNamedColumnValue<?>> newRows) {
            // do nothing
        }

        @Override
        public void putUpgradeMetadata(Multimap<UpgradeMetadataTable.UpgradeMetadataRow, ? extends UpgradeMetadataTable.UpgradeMetadataNamedColumnValue<?>> newRows) {
            // do nothing
        }
    }
}
