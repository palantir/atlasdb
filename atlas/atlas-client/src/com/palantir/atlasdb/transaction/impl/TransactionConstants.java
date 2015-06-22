// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.transaction.impl;


import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;


public class TransactionConstants {
    private TransactionConstants() {/* */}

    public static final String TRANSACTION_TABLE = "_transactions";
    public static final String COMMIT_TS_COLUMN_STRING = "t";
    public static final byte[] COMMIT_TS_COLUMN = PtBytes.toBytes("t");
    public static final long FAILED_COMMIT_TS = -1L;

    public static final long WARN_LEVEL_FOR_QUEUED_BYTES = 10*1024*1024;
    public static final long ERROR_LEVEL_FOR_QUEUED_BYTES = 10*WARN_LEVEL_FOR_QUEUED_BYTES;

    public static final long APPROX_IN_MEM_CELL_OVERHEAD_BYTES = 16;

    public static final byte[] getValueForTimestamp(long transactionTimestamp) {
        return EncodingUtils.encodeVarLong(transactionTimestamp);
    }

    public static final long getTimestampForValue(byte[] encodedTimestamp) {
        return EncodingUtils.decodeVarLong(encodedTimestamp);
    }

    public static final TableMetadata TRANSACTION_TABLE_METADATA = new TableMetadata(
        new NameMetadataDescription(ImmutableList.of(new NameComponentDescription("write_ts", ValueType.VAR_LONG))),
        new ColumnMetadataDescription(ImmutableList.of(
            new NamedColumnDescription(COMMIT_TS_COLUMN_STRING, "commit_ts", ColumnValueDescription.forType(ValueType.VAR_LONG)))),
        ConflictHandler.IGNORE_ALL);

}
