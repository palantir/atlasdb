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
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KeyValueServiceMigratorsTest {
    private static final TableReference TABLE_TO_MIGRATE =
            TableReference.create(Namespace.DEFAULT_NAMESPACE, "can-be-migrated");

    @Mock
    private KeyValueService fromKvs;

    @Test
    public void skipsTheOldScrubTable() {
        when(fromKvs.getAllTableNames())
                .thenReturn(ImmutableSet.of(AtlasDbConstants.OLD_SCRUB_TABLE, TABLE_TO_MIGRATE));

        Set<TableReference> migratableTableNames =
                KeyValueServiceMigratorUtils.getMigratableTableNames(fromKvs, ImmutableSet.of(), null);

        assertThat(migratableTableNames).containsExactly(TABLE_TO_MIGRATE);
    }

    @Test
    public void skipsSpecifiedCheckpointTable() {
        TableReference noNamespaceCheckpoint =
                TableReference.createWithEmptyNamespace(KeyValueServiceMigratorUtils.CHECKPOINT_TABLE_NAME);
        TableReference defaultNamespaceCheckpoint = TableReference.create(
                Namespace.create("old_namespace"), KeyValueServiceMigratorUtils.CHECKPOINT_TABLE_NAME);
        when(fromKvs.getAllTableNames())
                .thenReturn(ImmutableSet.of(noNamespaceCheckpoint, defaultNamespaceCheckpoint, TABLE_TO_MIGRATE));

        Set<TableReference> migratableTableNames = KeyValueServiceMigratorUtils.getMigratableTableNames(
                fromKvs, ImmutableSet.of(), defaultNamespaceCheckpoint);

        assertThat(migratableTableNames).containsExactlyInAnyOrder(TABLE_TO_MIGRATE, noNamespaceCheckpoint);
    }
}
