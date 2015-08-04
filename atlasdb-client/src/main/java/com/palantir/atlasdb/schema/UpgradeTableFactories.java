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
package com.palantir.atlasdb.schema;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.schema.generated.UpgTaskMetadataTable.UpgTaskMetadataNamedColumnValue;
import com.palantir.atlasdb.schema.generated.UpgTaskMetadataTable.UpgTaskMetadataRow;
import com.palantir.atlasdb.schema.generated.UpgradeMetadataTable.UpgradeMetadataNamedColumnValue;
import com.palantir.atlasdb.schema.generated.UpgradeMetadataTable.UpgradeMetadataRow;
import com.palantir.atlasdb.schema.generated.UpgradeTableFactory;
import com.palantir.atlasdb.schema.generated.UpgradeTableFactory.SharedTriggers;
import com.palantir.atlasdb.transaction.api.Transaction;

public final class UpgradeTableFactories {
    public static UpgradeTableFactory get() {
        SharedTriggers sharedTriggers = new SharedTriggers() {
            @Override
            public void putUpgTaskMetadata(Multimap<UpgTaskMetadataRow, ? extends UpgTaskMetadataNamedColumnValue<?>> newRows) {
                // WE DON'T INTEND TO MIGRATE UPDATES ANYMORE THAN TO UPGRADE MIGRATIONS
            }

            @Override
            public void putUpgradeMetadata(Multimap<UpgradeMetadataRow, ? extends UpgradeMetadataNamedColumnValue<?>> newRows) {
                // OTHERWISE IT WOULD BE TURTLES ON ELEPHANTS ON TURTLES ON ELEPHANTS
            }
        };
        return UpgradeTableFactory.of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(Functions.constant(sharedTriggers)));
    }
}
