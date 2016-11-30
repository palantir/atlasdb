/**
 * Copyright 2016 Palantir Technologies
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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public class OneNodeDownMetadataTest {

    @Test
    public void canGetMetadataForTable() {
        byte[] metadata = OneNodeDownTestSuite.db.getMetadataForTable(OneNodeDownTestSuite.TEST_TABLE);
        assertEquals(TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(AtlasDbConstants.GENERIC_TABLE_METADATA),
                TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadata));
    }

    @Test
    public void canGetMetadataForAll() {
        Map<TableReference, byte[]> metadataMap = OneNodeDownTestSuite.db.getMetadataForTables();
        assertEquals(TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(AtlasDbConstants.GENERIC_TABLE_METADATA),
                TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadataMap.get(OneNodeDownTestSuite.TEST_TABLE)));
    }

    @Test
    public void putMetadataForTableThrows() {
        TableMetadata newTableMetadata = new TableMetadata(new NameMetadataDescription(),
                new ColumnMetadataDescription(), ConflictHandler.IGNORE_ALL);
        assertThatThrownBy(() -> OneNodeDownTestSuite.db.putMetadataForTable(OneNodeDownTestSuite.TEST_TABLE,
                newTableMetadata.persistToBytes())).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void putMetadataForTablesThrows() {
        TableMetadata newTableMetadata = new TableMetadata(new NameMetadataDescription(),
                new ColumnMetadataDescription(), ConflictHandler.IGNORE_ALL);
        assertThatThrownBy(() -> OneNodeDownTestSuite.db.putMetadataForTables(
                ImmutableMap.of(OneNodeDownTestSuite.TEST_TABLE, newTableMetadata.persistToBytes())))
                .isInstanceOf(IllegalStateException.class);
    }
}
