/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.illiteracy;

import java.util.Optional;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.todo.TodoSchema;
import com.palantir.atlasdb.todo.generated.TodoSchemaTableFactory;
import com.palantir.atlasdb.todo.generated.WatchableStringMapTable;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.illiteracy.ImmutableRowPrefixReference;
import com.palantir.atlasdb.transaction.impl.illiteracy.ImmutableRowReference;
import com.palantir.atlasdb.transaction.impl.illiteracy.RowReference;
import com.palantir.atlasdb.transaction.impl.illiteracy.RowWatchAwareKeyValueService;
import com.palantir.atlasdb.transaction.impl.illiteracy.WatchRegistry;

public class SimpleRowWatchResource implements RowWatchResource {
    private static final Namespace NAMESPACE = TodoSchema.getSchema().getNamespace();
    private static final TableReference TABLE_REFERENCE = TableReference.create(
            NAMESPACE, WatchableStringMapTable.getRawTableName());

    private final TransactionManager transactionManager;
    private final WatchRegistry watchRegistry;

    private SimpleRowWatchResource(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
        this.watchRegistry = transactionManager.getWatchRegistry();
    }

    public static RowWatchResource create(TransactionManager transactionManager) {
        SimpleRowWatchResource simpleRowWatchResource = new SimpleRowWatchResource(transactionManager);
        transactionManager.getKeyValueService()
                .createTable(TABLE_REFERENCE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        return simpleRowWatchResource;
    }

    @Override
    public void beginWatching(String key) {
        watchRegistry.enableWatchForRows(singleKeyToRowReferenceSet(key));
    }

    @Override
    public void beginWatchingPrefix(String prefix) {
        watchRegistry.enableWatchForRowPrefix(
                ImmutableRowPrefixReference.builder()
                        .tableReference(TABLE_REFERENCE)
                        .prefix(WatchableStringMapTable.WatchableStringMapRow.of(prefix).persistToBytes())
                        .build());
    }

    private ImmutableSet<RowReference> singleKeyToRowReferenceSet(String key) {
        return ImmutableSet.of(ImmutableRowReference.builder()
                .tableReference(TABLE_REFERENCE)
                .row(WatchableStringMapTable.WatchableStringMapRow.of(key).persistToBytes())
                .build());
    }

    @Override
    public void endWatching(String key) {
        watchRegistry.disableWatchForRows(singleKeyToRowReferenceSet(key));
    }

    @Override
    public String get(String key) {
        Optional<WatchableStringMapTable.WatchableStringMapRowResult> result = transactionManager.runTaskWithRetry(
                tx -> {
                    WatchableStringMapTable table = TodoSchemaTableFactory.of(NAMESPACE).getWatchableStringMapTable(tx);
                    return table.getRow(WatchableStringMapTable.WatchableStringMapRow.of(key));
                });
        return result.map(WatchableStringMapTable.WatchableStringMapRowResult::getValue).orElse("");
    }

    @Override
    public void put(String key, StringWrapper value) {
        transactionManager.runTaskWithRetry(tx -> {
            WatchableStringMapTable table = TodoSchemaTableFactory.of(NAMESPACE).getWatchableStringMapTable(tx);
            table.putValue(WatchableStringMapTable.WatchableStringMapRow.of(key), value.str());
            return null;
        });
    }

    @Override
    public long getGetCount() {
        return ((RowWatchAwareKeyValueService) transactionManager.getKeyValueService()).getReadCount();
    }

    @Override
    public void resetGetCount() {
        ((RowWatchAwareKeyValueService) transactionManager.getKeyValueService()).resetReadCount();
    }

    @Override
    public void flushCache() {
        ((RowWatchAwareKeyValueService) transactionManager.getKeyValueService()).flushCache();
    }
}
