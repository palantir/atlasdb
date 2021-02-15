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
package com.palantir.atlasdb.keyvalue.jdbc;

import static org.jooq.impl.SQLDataType.BIGINT;
import static org.jooq.impl.SQLDataType.INTEGER;

import com.google.common.base.Function;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.RowN;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

public final class JdbcTimestampBoundStore implements TimestampBoundStore {
    private final JdbcKeyValueService kvs;
    private long latestTimestamp;

    private final Table<Record> TABLE;
    private static final Field<Integer> DUMMY_COLUMN = DSL.field("dummy_column", Integer.class);
    private static final Field<Long> LATEST_TIMESTAMP = DSL.field("latest_timestamp", Long.class);
    static final TableReference TIMESTAMP_TABLE = TableReference.createWithEmptyNamespace("_timestamp");

    private JdbcTimestampBoundStore(JdbcKeyValueService kvs) {
        this.kvs = kvs;
        TABLE = DSL.table(kvs.tableName(TIMESTAMP_TABLE));
    }

    public static JdbcTimestampBoundStore create(final JdbcKeyValueService kvs) {
        final JdbcTimestampBoundStore store = new JdbcTimestampBoundStore(kvs);
        kvs.run((Function<DSLContext, Void>) ctx -> {
            String partialSql = ctx.createTable(store.TABLE)
                    .column(DUMMY_COLUMN, INTEGER.nullable(false))
                    .column(LATEST_TIMESTAMP, BIGINT.nullable(false))
                    .getSQL();
            int endIndex = partialSql.lastIndexOf(')');
            String fullSql = partialSql.substring(0, endIndex) + "," + " CONSTRAINT "
                    + kvs.primaryKey(TIMESTAMP_TABLE) + " PRIMARY KEY ("
                    + DUMMY_COLUMN.getName() + ")" + partialSql.substring(endIndex);
            try {
                ctx.execute(fullSql);
            } catch (DataAccessException e) {
                kvs.handleTableCreationException(e);
            }
            ctx.insertInto(store.TABLE, DUMMY_COLUMN, LATEST_TIMESTAMP)
                    .select(ctx.select(DUMMY_COLUMN, LATEST_TIMESTAMP)
                            .from(kvs.values(
                                    ctx,
                                    new RowN[] {(RowN) DSL.row(0, 10000L)},
                                    "t",
                                    DUMMY_COLUMN.getName(),
                                    LATEST_TIMESTAMP.getName()))
                            .whereNotExists(ctx.selectOne().from(store.TABLE).where(DUMMY_COLUMN.eq(0))))
                    .execute();
            return null;
        });
        return store;
    }

    @Override
    public synchronized long getUpperLimit() {
        return kvs.run(ctx -> latestTimestamp = getLatestTimestamp(ctx));
    }

    @Override
    public synchronized void storeUpperLimit(final long limit) throws MultipleRunningTimestampServiceError {
        kvs.runInTransaction((Function<DSLContext, Void>) ctx -> {
            int rowsUpdated = ctx.update(TABLE)
                    .set(LATEST_TIMESTAMP, limit)
                    .where(DUMMY_COLUMN.eq(0).and(LATEST_TIMESTAMP.eq(latestTimestamp)))
                    .execute();
            if (rowsUpdated != 1) {
                long actualLatestTimestamp = getLatestTimestamp(ctx);
                throw new MultipleRunningTimestampServiceError(
                        "Timestamp limit changed underneath " + "us (limit in memory: "
                                + latestTimestamp + ", limit in db: " + actualLatestTimestamp
                                + "). This may indicate that another timestamp service is running against this db!");
            }
            latestTimestamp = limit;
            return null;
        });
    }

    private long getLatestTimestamp(DSLContext ctx) {
        return ctx.select(LATEST_TIMESTAMP)
                .from(TABLE)
                .where(DUMMY_COLUMN.eq(0))
                .fetchOne(LATEST_TIMESTAMP);
    }
}
