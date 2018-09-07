/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.cassandra.multinode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;

import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.exception.AtlasDbDependencyException;

public class TwoNodeDownTableManipulationTest {
        private static final TableReference NEW_TABLE = TableReference.createWithEmptyNamespace("new_table");
        private static final TableReference NEW_TABLE2 = TableReference.createWithEmptyNamespace("new_table2");
    private static final TableMetadata GENERIC = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(
            AtlasDbConstants.GENERIC_TABLE_METADATA);
        @Test
        @Ignore
        public void canCreateTable() {
            assertThat(OneNodeDownTestSuite.kvs.getAllTableNames()).doesNotContain(NEW_TABLE);
            OneNodeDownTestSuite.kvs.createTable(NEW_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);

            assertThat(OneNodeDownTestSuite.kvs.getAllTableNames()).contains(NEW_TABLE);
            OneNodeDownMetadataTest.assertContainsGenericMetadata(NEW_TABLE);
        }

    @Test
    public void canPutMetadataForTable() {
//        assertMetadataEmpty(OneNodeDownTestSuite.TEST_TABLE_FOR_METADATA);

        assertThatThrownBy(() -> OneNodeDownTestSuite.kvs.putMetadataForTable(OneNodeDownTestSuite.TEST_TABLE_FOR_METADATA,
                AtlasDbConstants.GENERIC_TABLE_METADATA)).isInstanceOf(AtlasDbDependencyException.class);
//        Mockito.verify(OneNodeDownTestSuite.kvs, Mockito.never()).putMetadataAndMaybeAlterTables(anyBoolean(), any(), any());
    }

    static void assertContainsGenericMetadata(TableReference tableRef) {
        assertEquals(GENERIC,
                TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(OneNodeDownTestSuite.kvs.getMetadataForTable(tableRef)));
    }

    static void assertMetadataEmpty(TableReference table) {
        assertArrayEquals(OneNodeDownTestSuite.kvs.getMetadataForTable(table), AtlasDbConstants.EMPTY_TABLE_METADATA);
    }
}
