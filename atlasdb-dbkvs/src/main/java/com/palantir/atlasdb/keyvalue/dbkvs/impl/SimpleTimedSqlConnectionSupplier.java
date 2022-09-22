/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import com.google.common.collect.ImmutableList;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.ConnectionSupplier;
import com.palantir.nexus.db.monitoring.timer.SqlTimer;
import com.palantir.nexus.db.monitoring.timer.SqlTimers;
import com.palantir.nexus.db.sql.ConnectionBackedSqlConnectionImpl;
import com.palantir.nexus.db.sql.SQL;
import com.palantir.nexus.db.sql.SqlConnection;
import com.palantir.nexus.db.sql.SqlConnectionHelper;

public final class SimpleTimedSqlConnectionSupplier implements SqlConnectionSupplier {
    private final ConnectionSupplier connectionSupplier;
    private final SQL sql;

    public SimpleTimedSqlConnectionSupplier(ConnectionSupplier connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
        this.sql = new SimpleSql();
    }

    @Override
    public SqlConnection get() {
        return new ConnectionBackedSqlConnectionImpl(
                connectionSupplier.get(),
                () -> {
                    throw new UnsupportedOperationException("This SQL connection does not provide reliable timestamp.");
                },
                new SqlConnectionHelper(sql));
    }

    @Override
    public void close() throws PalantirSqlException {
        connectionSupplier.close();
    }

    private static final class SimpleSql extends SQL {
        @Override
        protected SqlConfig getSqlConfig() {
            return new SqlConfig() {
                @Override
                public boolean isSqlCancellationDisabled() {
                    return false;
                }

                @Override
                public SqlTimer getSqlTimer() {
                    return SqlTimers.createCombinedSqlTimer(getSqlTimers());
                }

                private Iterable<SqlTimer> getSqlTimers() {
                    return ImmutableList.of(SqlTimers.createDurationSqlTimer(), SqlTimers.createSqlStatsSqlTimer());
                }
            };
        }
    }
}
