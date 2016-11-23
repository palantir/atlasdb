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
package com.palantir.atlasdb.keyvalue.cassandra;

import java.nio.ByteBuffer;

import org.apache.cassandra.thrift.Column;

import com.palantir.atlasdb.encoding.PtBytes;

public class CassandraTimestampUtils {
    private CassandraTimestampUtils() {
        // utility
    }

    public static Column makeColumn(long ts) {
        Column col = new Column();
        col.setName(getColumnName());
        col.setValue(PtBytes.toBytes(ts));
        col.setTimestamp(CassandraTimestampConstants.CASSANDRA_TIMESTAMP);
        return col;
    }

    public static byte[] getColumnName() {
        return CassandraKeyValueServices
                .makeCompositeBuffer(PtBytes.toBytes(CassandraTimestampConstants.ROW_AND_COLUMN_NAME),
                        CassandraTimestampConstants.CASSANDRA_TIMESTAMP)
                .array();
    }

    public static ByteBuffer getRowName() {
        return ByteBuffer.wrap(PtBytes.toBytes(CassandraTimestampConstants.ROW_AND_COLUMN_NAME));
    }
}
