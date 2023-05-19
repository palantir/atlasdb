/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.store;

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers;
import com.palantir.atlasdb.workload.util.AtlasDbUtils;
import com.palantir.logsafe.SafeArg;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.Test;

public final class AtlasDbTransactionStoreFactoryTest {

    private final TransactionManager transactionManager = TransactionManagers.createInMemory(Set.of());
    private final AtlasDbTransactionStoreFactory defaultFactory =
            new AtlasDbTransactionStoreFactory(transactionManager, Optional.empty());

    @Test
    public void createWillCreateProvidedTablesAndIndexes() {
        AtlasDbTransactionStoreFactory factory = new AtlasDbTransactionStoreFactory(
                transactionManager,
                Optional.of(WorkloadTestHelpers.TABLE_REFERENCE.getNamespace().getName()));
        factory.create(
                Map.of(WorkloadTestHelpers.TABLE_1, IsolationLevel.SERIALIZABLE),
                Set.of(createIndexTable(WorkloadTestHelpers.TABLE_1_INDEX_1, WorkloadTestHelpers.TABLE_1)));
        assertThat(transactionManager.getKeyValueService().getAllTableNames())
                .contains(WorkloadTestHelpers.TABLE_REFERENCE, WorkloadTestHelpers.INDEX_REFERENCE);
    }

    @Test
    public void createTableReferenceWithEmptyNamespaceIfNoNamespaceProvided() {
        assertThat(defaultFactory
                        .createTableReference(WorkloadTestHelpers.TABLE_1)
                        .getNamespace()
                        .isEmptyNamespace())
                .isTrue();
    }

    @Test
    public void createTableReferenceHasNamespaceWhenProvided() {
        AtlasDbTransactionStoreFactory factory = new AtlasDbTransactionStoreFactory(
                transactionManager, Optional.of(WorkloadTestHelpers.NAMESPACE.getName()));
        assertThat(factory.createTableReference(WorkloadTestHelpers.TABLE_1).getNamespace())
                .isEqualTo(WorkloadTestHelpers.NAMESPACE);
    }

    @Test
    public void toAtlasTablesConvertsSerializableTablesCorrectly() {
        assertThat(defaultFactory.toAtlasTables(
                        Map.of(WorkloadTestHelpers.TABLE_1, IsolationLevel.SERIALIZABLE), Set.of()))
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        defaultFactory.createTableReference(WorkloadTestHelpers.TABLE_1),
                        AtlasDbUtils.tableMetadata(ConflictHandler.SERIALIZABLE)));
    }

    @Test
    public void toAtlasTablesConvertsSerializableIndexTablesCorrectly() {
        assertThat(defaultFactory.toAtlasTables(
                        Map.of(WorkloadTestHelpers.TABLE_1, IsolationLevel.SERIALIZABLE),
                        Set.of(createIndexTable(WorkloadTestHelpers.TABLE_1_INDEX_1, WorkloadTestHelpers.TABLE_1))))
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        defaultFactory.createTableReference(WorkloadTestHelpers.TABLE_1),
                        AtlasDbUtils.tableMetadata(ConflictHandler.SERIALIZABLE),
                        defaultFactory.createTableReference(WorkloadTestHelpers.TABLE_1_INDEX_1),
                        AtlasDbUtils.indexMetadata(ConflictHandler.SERIALIZABLE)));
    }

    @Test
    public void toAtlasTablesHandlesMultipleTablesAndIndexesCorrectly() {
        assertThat(defaultFactory.toAtlasTables(
                        Map.of(
                                WorkloadTestHelpers.TABLE_1,
                                IsolationLevel.SERIALIZABLE,
                                WorkloadTestHelpers.TABLE_2,
                                IsolationLevel.SNAPSHOT),
                        Set.of(
                                createIndexTable(WorkloadTestHelpers.TABLE_1_INDEX_1, WorkloadTestHelpers.TABLE_1),
                                createIndexTable(WorkloadTestHelpers.TABLE_1_INDEX_2, WorkloadTestHelpers.TABLE_1),
                                createIndexTable(WorkloadTestHelpers.TABLE_2_INDEX_1, WorkloadTestHelpers.TABLE_2))))
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        defaultFactory.createTableReference(WorkloadTestHelpers.TABLE_1),
                        AtlasDbUtils.tableMetadata(ConflictHandler.SERIALIZABLE),
                        defaultFactory.createTableReference(WorkloadTestHelpers.TABLE_1_INDEX_1),
                        AtlasDbUtils.indexMetadata(ConflictHandler.SERIALIZABLE),
                        defaultFactory.createTableReference(WorkloadTestHelpers.TABLE_1_INDEX_2),
                        AtlasDbUtils.indexMetadata(ConflictHandler.SERIALIZABLE),
                        defaultFactory.createTableReference(WorkloadTestHelpers.TABLE_2),
                        AtlasDbUtils.tableMetadata(ConflictHandler.RETRY_ON_WRITE_WRITE),
                        defaultFactory.createTableReference(WorkloadTestHelpers.TABLE_2_INDEX_1),
                        AtlasDbUtils.tableMetadata(ConflictHandler.IGNORE_ALL)));
    }

    @Test
    public void throwsWhenIndexReferencesUnknownPrimaryTable() {
        IndexTable goodIndexTable = createIndexTable(WorkloadTestHelpers.TABLE_1_INDEX_2, WorkloadTestHelpers.TABLE_1);
        IndexTable badIndexTable = createIndexTable(WorkloadTestHelpers.TABLE_1_INDEX_1, "blah");
        assertThatLoggableExceptionThrownBy(() -> defaultFactory.create(
                        Map.of(WorkloadTestHelpers.TABLE_1, IsolationLevel.SERIALIZABLE),
                        Set.of(goodIndexTable, badIndexTable)))
                .hasMessageContaining("Found indexes which reference an unknown primary table")
                .hasExactlyArgs(SafeArg.of("indexesWithUnknownPrimaryTables", Set.of(badIndexTable)));
    }

    @Test
    public void throwsWhenPrimaryAndIndexTableHaveConflictingNames() {
        assertThatLoggableExceptionThrownBy(() -> defaultFactory.create(
                        Map.of(
                                WorkloadTestHelpers.TABLE_1,
                                IsolationLevel.SERIALIZABLE,
                                WorkloadTestHelpers.TABLE_1_INDEX_1,
                                IsolationLevel.SERIALIZABLE),
                        Set.of(createIndexTable(
                                WorkloadTestHelpers.TABLE_1.toUpperCase(Locale.ROOT),
                                WorkloadTestHelpers.TABLE_1_INDEX_1))))
                .hasMessageContaining("Found indexes which have the same name as primary tables")
                .hasExactlyArgs(SafeArg.of("conflictingTableNames", Set.of(WorkloadTestHelpers.TABLE_1)));
    }

    @Test
    public void throwsWhenIndexTablesHaveDuplicateNames() {
        assertThatLoggableExceptionThrownBy(() -> defaultFactory.create(
                        Map.of(
                                WorkloadTestHelpers.TABLE_1,
                                IsolationLevel.SERIALIZABLE,
                                WorkloadTestHelpers.TABLE_2,
                                IsolationLevel.SERIALIZABLE),
                        Set.of(
                                createIndexTable(
                                        WorkloadTestHelpers.TABLE_1_INDEX_1.toUpperCase(Locale.ROOT),
                                        WorkloadTestHelpers.TABLE_1),
                                createIndexTable(WorkloadTestHelpers.TABLE_1_INDEX_1, WorkloadTestHelpers.TABLE_2))))
                .hasMessageContaining("Found index tables with the same name")
                .hasExactlyArgs(SafeArg.of("duplicateTableIndexNames", Set.of(WorkloadTestHelpers.TABLE_1_INDEX_1)));
    }

    private IndexTable createIndexTable(String indexTableName, String primaryTableName) {
        return IndexTable.builder()
                .indexTableName(indexTableName)
                .primaryTableName(primaryTableName)
                .build();
    }
}
