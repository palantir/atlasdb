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

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.schema.generated.UpgTaskMetadataTable;
import com.palantir.atlasdb.schema.generated.UpgTaskMetadataTable.UpgTaskMetadataRow;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;

public class UpgradeTaskCheckpointer extends AbstractTaskCheckpointer {
    private final Namespace namespace;
    private final SchemaVersion version;

    public UpgradeTaskCheckpointer(Namespace namespace,
                                   SchemaVersion version,
                                   TransactionManager txManager) {
        super(txManager);
        this.namespace = namespace;
        this.version = version;
    }

    @Override
    public void checkpoint(String extraId, long rangeId, byte[] nextRowName, Transaction t) {
        UpgTaskMetadataRow row = getRow(extraId, rangeId);
        byte[] value = toDb(nextRowName, false);
        UpgradeTableFactories.get().getUpgTaskMetadataTable(t).putStart(row, value);
    }

    @Override
    public byte[] getCheckpoint(String extraId, long rangeId, Transaction t) {
        UpgTaskMetadataRow row = getRow(extraId, rangeId);
        byte[] value = UpgradeTableFactories.get().getUpgTaskMetadataTable(t).getStarts(ImmutableSet.of(row)).get(row);
        return fromDb(value);
    }

    @Override
    public void createCheckpoints(final String extraId,
                                  final Map<Long, byte[]> startById) {
        txManager.runTaskWithRetry(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction t) {
                Set<UpgTaskMetadataRow> rows = Sets.newHashSet();
                for (long rangeId : startById.keySet()) {
                    rows.add(getRow(extraId, rangeId));
                }

                UpgTaskMetadataTable table = UpgradeTableFactories.get().getUpgTaskMetadataTable(t);
                Map<UpgTaskMetadataRow, byte[]> starts = table.getStarts(rows);

                if (starts.isEmpty()) {
                    for (Entry<Long, byte[]> e : startById.entrySet()) {
                        UpgTaskMetadataRow row = getRow(extraId, e.getKey());
                        byte[] value = toDb(e.getValue(), true);
                        table.putStart(row, value);
                    }
                }
                return null;
            }
        });
    }

    @Override
    public void deleteCheckpoints() {
        txManager.runTaskWithRetry(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction t) {
                RangeRequest range = UpgTaskMetadataRow.createPrefixRangeUnsorted(
                        namespace.getName(),
                        version.getVersion(),
                        version.getHotfix(),
                        version.getHotfixHotfix()).build();
                UpgradeTableFactories.get().getUpgTaskMetadataTable(t).deleteRange(range);
                return null;
            }
        });
    }

    private UpgTaskMetadataRow getRow(String extraId, long rangeId) {
        return UpgTaskMetadataRow.of(
                namespace.getName(),
                version.getVersion(),
                version.getHotfix(),
                version.getHotfixHotfix(),
                extraId,
                rangeId);
    }
}
