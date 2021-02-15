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
package com.palantir.atlasdb.keyvalue.jdbc.impl;

import static org.jooq.impl.DSL.field;

import org.jooq.Field;

public final class JdbcConstants {

    private JdbcConstants() {
        // cannot instantiate
    }

    public static final Field<String> TABLE_NAME = field("table_name", String.class);
    public static final Field<byte[]> METADATA = field("metadata", byte[].class);

    public static final String ROW_NAME = "row_name";
    public static final String COL_NAME = "col_name";
    public static final String TIMESTAMP = "timestamp";
    public static final String VALUE = "value";

    public static final String ATLAS_TABLE = "a";
    public static final Field<byte[]> A_ROW_NAME = field(ATLAS_TABLE + '.' + ROW_NAME, byte[].class);
    public static final Field<byte[]> A_COL_NAME = field(ATLAS_TABLE + '.' + COL_NAME, byte[].class);
    public static final Field<Long> A_TIMESTAMP = field(ATLAS_TABLE + '.' + TIMESTAMP, Long.class);
    public static final Field<byte[]> A_VALUE = field(ATLAS_TABLE + '.' + VALUE, byte[].class);

    public static final String RANGE_TABLE = "r";
    public static final Field<byte[]> R_ROW_NAME = field(RANGE_TABLE + '.' + ROW_NAME, byte[].class);
    public static final Field<byte[]> R_COL_NAME = field(RANGE_TABLE + '.' + COL_NAME, byte[].class);
    public static final Field<Long> R_TIMESTAMP = field(RANGE_TABLE + '.' + TIMESTAMP, Long.class);
    public static final Field<byte[]> R_VALUE = field(RANGE_TABLE + '.' + VALUE, byte[].class);

    public static final String TEMP_TABLE_1 = "t1";
    public static final Field<byte[]> T1_ROW_NAME = field(TEMP_TABLE_1 + '.' + ROW_NAME, byte[].class);
    public static final Field<byte[]> T1_COL_NAME = field(TEMP_TABLE_1 + '.' + COL_NAME, byte[].class);
    public static final Field<Long> T1_TIMESTAMP = field(TEMP_TABLE_1 + '.' + TIMESTAMP, Long.class);
    public static final Field<byte[]> T1_VALUE = field(TEMP_TABLE_1 + '.' + VALUE, byte[].class);

    public static final String TEMP_TABLE_2 = "t2";
    public static final Field<byte[]> T2_ROW_NAME = field(TEMP_TABLE_2 + '.' + ROW_NAME, byte[].class);
    public static final Field<byte[]> T2_COL_NAME = field(TEMP_TABLE_2 + '.' + COL_NAME, byte[].class);
    public static final Field<Long> T2_TIMESTAMP = field(TEMP_TABLE_2 + '.' + TIMESTAMP, Long.class);
    public static final Field<byte[]> T2_VALUE = field(TEMP_TABLE_2 + '.' + VALUE, byte[].class);

    public static final String MAX_TIMESTAMP = "max_timestamp";
    public static final Field<Long> T2_MAX_TIMESTAMP = field(TEMP_TABLE_2 + '.' + MAX_TIMESTAMP, Long.class);
}
