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
package com.palantir.atlasdb.keyvalue.jdbc;

import static org.jooq.impl.SQLDataType.BIGINT;
import static org.jooq.impl.SQLDataType.INTEGER;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.google.common.base.Function;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;

public class JdbcTimestampBoundStore implements TimestampBoundStore {
    private final JdbcKeyValueService kvs;
    private long latestTimestamp;

    private final Table<Record> TABLE;
    private static final Field<Integer> DUMMY_COLUMN = DSL.field("dummy_column", Integer.class);
    private static final Field<Long> LATEST_TIMESTAMP = DSL.field("latest_timestamp", Long.class);

    private JdbcTimestampBoundStore(JdbcKeyValueService kvs) {
        this.kvs = kvs;
        TABLE = kvs.atlasTable("_timestamp");
    }

    public static JdbcTimestampBoundStore create(JdbcKeyValueService kvs) {
        final JdbcTimestampBoundStore store = new JdbcTimestampBoundStore(kvs);
        kvs.run(new Function<DSLContext, Void>() {
            @Override
            public Void apply(DSLContext ctx) {
                String partialSql = ctx.createTable(store.TABLE)
                        .column(DUMMY_COLUMN, INTEGER.nullable(false))
                        .column(LATEST_TIMESTAMP, BIGINT.nullable(false))
                        .getSQL();
                int endIndex = partialSql.lastIndexOf(')');
                String fullSql = partialSql.substring(0, endIndex) + "," +
                        " CONSTRAINT pk_timestamp" +
                        " PRIMARY KEY (" + DUMMY_COLUMN.getName() + ")" +
                        partialSql.substring(endIndex);
                try {
                    ctx.execute(fullSql);
                } catch (DataAccessException e) {
                    // TODO: check if it's because the table already exists.
                }
                ctx.insertInto(store.TABLE, DUMMY_COLUMN, LATEST_TIMESTAMP)
                    .values(0, 10000L)
                    .onDuplicateKeyIgnore()
                    .execute();
                return null;
            }
        });
        return store;
    }

    @Override
    public synchronized long getUpperLimit() {
        return kvs.run(new Function<DSLContext, Long>() {
            @Override
            public Long apply(DSLContext ctx) {
                return latestTimestamp = ctx.select(LATEST_TIMESTAMP)
                        .from(TABLE)
                        .where(DUMMY_COLUMN.eq(0))
                        .fetchOne(LATEST_TIMESTAMP);
            }
        });
    }

    @Override
    public synchronized void storeUpperLimit(final long limit) throws MultipleRunningTimestampServiceError {
        kvs.runInTransaction(new Function<DSLContext, Void>() {
            @Override
            public Void apply(DSLContext ctx) {
                long actualLatestTimestamp = ctx.select(LATEST_TIMESTAMP)
                        .from(TABLE)
                        .where(DUMMY_COLUMN.eq(0))
                        .forUpdate()
                        .fetchOne(LATEST_TIMESTAMP);
                if (latestTimestamp != actualLatestTimestamp) {
                    throw new MultipleRunningTimestampServiceError("Timestamp limit changed underneath " +
                            "us (limit in memory: " + latestTimestamp + ", limit in db: " + actualLatestTimestamp +
                            "). This may indicate that another timestamp service is running against this db!");
                }
                latestTimestamp = limit;
                ctx.update(TABLE)
                    .set(LATEST_TIMESTAMP, limit)
                    .where(DUMMY_COLUMN.eq(0))
                    .execute();
                return null;
            }
        });
    }
}
