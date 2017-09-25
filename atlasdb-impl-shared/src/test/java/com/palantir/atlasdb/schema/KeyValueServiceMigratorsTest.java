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

package com.palantir.atlasdb.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class KeyValueServiceMigratorsTest {
    @Test
    public void skipsTheOldScrubTable() {
        TableReference tableToMigrate = TableReference.create(Namespace.DEFAULT_NAMESPACE, "can-be-migrated");

        KeyValueService fromKvs = mock(KeyValueService.class);
        when(fromKvs.getAllTableNames()).thenReturn(ImmutableSet.of(AtlasDbConstants.OLD_SCRUB_TABLE, tableToMigrate));

        Set<TableReference> creatableTableNames = KeyValueServiceMigrators.getMigratableTableNames(fromKvs,
                ImmutableSet.of());

        assertThat(creatableTableNames).containsExactly(tableToMigrate);
    }
}
