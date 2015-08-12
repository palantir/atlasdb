/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.rdbms.utils;

public final class Columns {

    public static class ColumnQueryToken {
        private final String content;
        private ColumnQueryToken(String content) {
            this.content = content;
        }

        @Override
        public String toString() {
            return content;
        }

        public ColumnQueryToken eq(ColumnQueryToken other) {
            return new ColumnQueryToken(content + "=" + other.content);
        }

        public ColumnQueryToken lt(ColumnQueryToken other) {
            return new ColumnQueryToken(content + "<" + other.content);
        }

        public ColumnQueryToken and(ColumnQueryToken other) {
            return new ColumnQueryToken(content + " AND " + other.content);
        }

        public ColumnQueryToken append(ColumnQueryToken other) {
            return new ColumnQueryToken(content + "," + other.content);
        }

        public ColumnQueryToken appendSpace() {
            return new ColumnQueryToken(content + " ");
        }
    }

    public static final ColumnQueryToken TIMESTAMP = new ColumnQueryToken("atlasdb_timestamp");
    public static final ColumnQueryToken CONTENT = new ColumnQueryToken("atlasdb_content");
    public static final ColumnQueryToken ROW = new ColumnQueryToken("atlasdb_row");
    public static final ColumnQueryToken COLUMN = new ColumnQueryToken("atlasdb_column");

    private static final String maybeEmpty(String tableName) {
        if (tableName.equals("")) {
            return "";
        }
        return tableName + ".";
    }

    public static final ColumnQueryToken ROW(String tableName) {
        return new ColumnQueryToken(maybeEmpty(tableName) + ROW);
    }

    public static final ColumnQueryToken COLUMN(String tableName) {
        return new ColumnQueryToken(maybeEmpty(tableName) + COLUMN);
    }

    public static final ColumnQueryToken TIMESTAMP(String tableName) {
        return new ColumnQueryToken(maybeEmpty(tableName) + TIMESTAMP);
    }

    public static final ColumnQueryToken CONTENT(String tableName) {
        return new ColumnQueryToken(maybeEmpty(tableName) + CONTENT);
    }

    public static final String ROW_COLUMN_TIMESTAMP_AS(String tableName) {
        return ROW(tableName) + " AS " + ROW + ", " + COLUMN(tableName) + " AS " + COLUMN + ", " + TIMESTAMP(tableName) + " AS " + TIMESTAMP;
    }

    public static final String ROW_COLUMN_TIMESTAMP_CONTENT_AS(String tableName) {
        return ROW_COLUMN_TIMESTAMP_AS(tableName) + ", " + CONTENT(tableName) + " AS " + CONTENT;
    }

    private Columns() {}
}
