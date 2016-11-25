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

import static org.junit.Assert.assertEquals;

import static com.palantir.cassandra.multinode.OneNodeDownTestSuite.TEST_TABLE;
import static com.palantir.cassandra.multinode.OneNodeDownTestSuite.db;

import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.ConflictHandler;


public class OneNodeDownMetadataTest {
    @Rule
    public ExpectedException expect_exception = ExpectedException.none();

    @Test
    public void canGetMetadataForTable(){
        byte[] metadata = db.getMetadataForTable(TEST_TABLE);
        assertEquals(TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(AtlasDbConstants.GENERIC_TABLE_METADATA), TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadata));
    }

    @Test
    public void canGetMetadataForAll(){
        Map<TableReference, byte[]> metadataMap = db.getMetadataForTables();
        assertEquals(1, metadataMap.size());
        assertEquals(TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(AtlasDbConstants.GENERIC_TABLE_METADATA), TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadataMap.get(TEST_TABLE)));
    }

    @Test
    public void putMetadataForTableThrowsISE(){
        //All Cassadra nodes must agree on schema version (fails when a node is down)
        expect_exception.expect(IllegalStateException.class);
        TableMetadata newTableMetadata = new TableMetadata(new NameMetadataDescription(),
                new ColumnMetadataDescription(), ConflictHandler.IGNORE_ALL);
        db.putMetadataForTable(TEST_TABLE, newTableMetadata.persistToBytes());
//        byte[] metadata = db.getMetadataForTable(TEST_TABLE);
//        assertNotEquals(TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(AtlasDbConstants.GENERIC_TABLE_METADATA), TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadata));
//        assertEquals(newTableMetadata, TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadata));
//        db.putMetadataForTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    @Test
    public void putMetadataForTablesThrowsISE(){
        expect_exception.expect(IllegalStateException.class);
        TableMetadata newTableMetadata = new TableMetadata(new NameMetadataDescription(),
                new ColumnMetadataDescription(), ConflictHandler.IGNORE_ALL);
        db.putMetadataForTables(ImmutableMap.of(TEST_TABLE, newTableMetadata.persistToBytes()));
    }
}
