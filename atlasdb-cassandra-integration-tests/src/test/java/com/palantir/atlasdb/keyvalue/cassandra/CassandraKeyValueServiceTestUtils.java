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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public final class CassandraKeyValueServiceTestUtils {
    private CassandraKeyValueServiceTestUtils() {
        // utility
    }

    // notably, this metadata is different from the default AtlasDbConstants.GENERIC_TABLE_METADATA
    // to make sure the tests are actually exercising the correct retrieval codepaths
    public static final byte[] ORIGINAL_METADATA = TableMetadata.builder()
            .conflictHandler(ConflictHandler.RETRY_ON_VALUE_CHANGED)
            .nameLogSafety(TableMetadataPersistence.LogSafety.SAFE)
            .build()
            .persistToBytes();

    public static void clearOutMetadataTable(KeyValueService kvs) {
        kvs.truncateTable(AtlasDbConstants.DEFAULT_METADATA_TABLE);
    }

    public static void insertGenericMetadataIntoLegacyCell(KeyValueService kvs, TableReference tableRef) {
        insertGenericMetadataIntoLegacyCell(kvs, tableRef, AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    public static void insertGenericMetadataIntoLegacyCell(KeyValueService kvs, TableReference tableRef, byte[] data) {
        Cell legacyMetadataCell = CassandraKeyValueServices.getOldMetadataCell(tableRef);
        kvs.put(
                AtlasDbConstants.DEFAULT_METADATA_TABLE,
                ImmutableMap.of(legacyMetadataCell, data),
                System.currentTimeMillis());
    }
}
