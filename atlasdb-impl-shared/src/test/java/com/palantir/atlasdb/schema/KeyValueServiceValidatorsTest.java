/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.schema;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import org.junit.Test;

public class KeyValueServiceValidatorsTest {
    private static final TableReference SWEEP_PRIORITY = TableReference.create(
            SweepSchema.INSTANCE.getNamespace(), SweepPriorityTable.getRawTableName());
    private static final TableReference OTHER_PRIORITY = TableReference.create(
            Namespace.create("foo"), SweepPriorityTable.getRawTableName());

    private final KeyValueService kvs = new InMemoryKeyValueService(true);

    @Test
    public void sweepPriorityTableIsASweepTable() {
        assertThat(KeyValueServiceValidators.isSweepTableReference(SWEEP_PRIORITY)).isTrue();
    }

    @Test
    public void otherPriorityTableIsNotASweepTable() {
        assertThat(KeyValueServiceValidators.isSweepTableReference(OTHER_PRIORITY)).isFalse();
    }

    @Test
    public void sweepPriorityTableNotValidated() {
        kvs.createTable(SWEEP_PRIORITY, AtlasDbConstants.EMPTY_TABLE_METADATA);
        assertThat(KeyValueServiceValidators.getValidatableTableNames(kvs, ImmutableSet.of())).isEmpty();
    }

    @Test
    public void transactionTablesNotValidated() {
        TransactionTables.createTables(kvs);
        assertThat(KeyValueServiceValidators.getValidatableTableNames(kvs, ImmutableSet.of())).isEmpty();
    }

    @Test
    public void otherPriorityTableIsValidated() {
        kvs.createTable(OTHER_PRIORITY, AtlasDbConstants.EMPTY_TABLE_METADATA);
        assertThat(KeyValueServiceValidators.getValidatableTableNames(kvs, ImmutableSet.of()))
                .containsExactly(OTHER_PRIORITY);
    }

    @Test
    public void unmigratableTablesAreNotValidated() {
        kvs.createTable(OTHER_PRIORITY, AtlasDbConstants.EMPTY_TABLE_METADATA);
        assertThat(KeyValueServiceValidators.getValidatableTableNames(kvs, ImmutableSet.of(OTHER_PRIORITY))).isEmpty();
    }
}
