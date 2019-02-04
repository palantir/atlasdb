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
package com.palantir.atlasdb.transaction.impl;


import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;


public class TransactionConstants {
    private TransactionConstants() {/* */}

    public static final TableReference TRANSACTION_TABLE = TableReference.createWithEmptyNamespace("_transactions");
    public static final TableReference TRANSACTIONS2_TABLE = TableReference.createWithEmptyNamespace("_transactions2");

    public static final String COMMIT_TS_COLUMN_STRING = "t";
    public static final byte[] COMMIT_TS_COLUMN = PtBytes.toBytes(COMMIT_TS_COLUMN_STRING);
    public static final long FAILED_COMMIT_TS = -1L;

    public static final long WARN_LEVEL_FOR_QUEUED_BYTES = 10 * 1024 * 1024;

    public static final long APPROX_IN_MEM_CELL_OVERHEAD_BYTES = 16;

    // DO NOT change without a transactions table migration!
    public static final int V2_TRANSACTION_NUM_PARTITIONS = 16;

    public static final int DIRECT_ENCODING_TRANSACTIONS_SCHEMA_VERSION = 1;
    public static final int TICKETS_ENCODING_TRANSACTIONS_SCHEMA_VERSION = 2;
    public static final Set<Integer> SUPPORTED_TRANSACTIONS_SCHEMA_VERSIONS = ImmutableSet.of(
            DIRECT_ENCODING_TRANSACTIONS_SCHEMA_VERSION,
            TICKETS_ENCODING_TRANSACTIONS_SCHEMA_VERSION);

    public static byte[] getValueForTimestamp(long transactionTimestamp) {
        return EncodingUtils.encodeVarLong(transactionTimestamp);
    }

    public static long getTimestampForValue(byte[] encodedTimestamp) {
        return EncodingUtils.decodeVarLong(encodedTimestamp);
    }

    public static final TableMetadata TRANSACTION_TABLE_METADATA = new TableMetadata(
            NameMetadataDescription.create(ImmutableList.of(new NameComponentDescription.Builder()
                    .componentName("write_ts")
                    .type(ValueType.VAR_LONG)
                    .build())),
            new ColumnMetadataDescription(ImmutableList.of(
                    new NamedColumnDescription(COMMIT_TS_COLUMN_STRING, "commit_ts",
                            ColumnValueDescription.forType(ValueType.VAR_LONG)))),
            ConflictHandler.IGNORE_ALL,
            TableMetadataPersistence.LogSafety.SAFE);


    public static final TableMetadata TRANSACTIONS2_TABLE_METADATA = new TableDefinition() {{
        allSafeForLoggingByDefault();
        rowName();
        rowComponent("start_ts_row", ValueType.BLOB);
        dynamicColumns();
        columnComponent("start_ts_col", ValueType.BLOB);
        value(ValueType.BLOB);
        sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING);
        conflictHandler(ConflictHandler.IGNORE_ALL);
    }}.toTableMetadata();
}
