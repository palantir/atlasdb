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

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;

public final class TransactionConstants {
    private TransactionConstants() {
        /* */
    }

    public static final TableReference TRANSACTION_TABLE = TableReference.createWithEmptyNamespace("_transactions");
    public static final TableReference TRANSACTIONS2_TABLE = TableReference.createWithEmptyNamespace("_transactions2");

    // When we do this for prod we should probably just use a leading byte in the transactions2 table, and/or something
    // that would not deserialize into a VAR_LONG. For hackweek I'll have multiple tables though.
    public static final TableReference TRANSACTIONS3_TABLE = TableReference.createWithEmptyNamespace("_transactions3");

    public static final String COMMIT_TS_COLUMN_STRING = "t";
    public static final byte[] COMMIT_TS_COLUMN = PtBytes.toBytes(COMMIT_TS_COLUMN_STRING);
    public static final long FAILED_COMMIT_TS = -1L;

    public static final long WARN_LEVEL_FOR_QUEUED_BYTES = 10 * 1024 * 1024;

    public static final long APPROX_IN_MEM_CELL_OVERHEAD_BYTES = 16;

    // DO NOT change without a transactions table migration!
    public static final int V2_TRANSACTION_NUM_PARTITIONS = 16;

    public static final int DIRECT_ENCODING_TRANSACTIONS_SCHEMA_VERSION = 1;
    public static final int TICKETS_ENCODING_TRANSACTIONS_SCHEMA_VERSION = 2;
    public static final int JOINT_SUPPORTING_VERSION = 3;
    public static final ImmutableSet<Integer> SUPPORTED_TRANSACTIONS_SCHEMA_VERSIONS =
            ImmutableSet.of(DIRECT_ENCODING_TRANSACTIONS_SCHEMA_VERSION, TICKETS_ENCODING_TRANSACTIONS_SCHEMA_VERSION,
                    JOINT_SUPPORTING_VERSION);

    public static byte[] getValueForTimestamp(long transactionTimestamp) {
        return EncodingUtils.encodeVarLong(transactionTimestamp);
    }

    public static long getTimestampForValue(byte[] encodedTimestamp) {
        return EncodingUtils.decodeVarLong(encodedTimestamp);
    }

    public static final TableMetadata TRANSACTION_TABLE_METADATA = TableMetadata.internal()
            .singleRowComponent("write_ts", ValueType.VAR_LONG)
            .singleNamedColumn(COMMIT_TS_COLUMN_STRING, "commit_ts", ValueType.VAR_LONG)
            .nameLogSafety(LogSafety.SAFE)
            .sweepStrategy(SweepStrategy.NOTHING)
            .build();

    /**
     * Metadata for the _transactions2 table.
     *
     * In internal benchmarking, an explicit compression block size of 64, along with very low bloom filter
     * false-positive chances and a low index interval were found to be very beneficial for reads.
     */
    public static final TableMetadata TRANSACTIONS2_TABLE_METADATA = TableMetadata.internal()
            .singleSafeRowComponent("start_ts_row", ValueType.BLOB)
            .singleDynamicSafeColumn("start_ts_col", ValueType.BLOB, ValueType.BLOB)
            .nameLogSafety(LogSafety.SAFE)
            .sweepStrategy(SweepStrategy.NOTHING)
            .explicitCompressionBlockSizeKB(64)
            .denselyAccessedWideRows(true)
            .build();

    /**
     * Metadata for the _transactions3 table.
     *
     * We took the same things from transactions2, as the partitioning / structure is similar.
     */
    public static final TableMetadata TRANSACTIONS3_TABLE_METADATA = TableMetadata.internal()
            .singleSafeRowComponent("start_ts_row", ValueType.BLOB)
            .singleDynamicSafeColumn("start_ts_col", ValueType.BLOB, ValueType.BLOB)
            .nameLogSafety(LogSafety.SAFE)
            .sweepStrategy(SweepStrategy.NOTHING)
            .explicitCompressionBlockSizeKB(64)
            .denselyAccessedWideRows(true)
            .build();
}
