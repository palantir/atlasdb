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
package com.palantir.atlasdb.keyvalue.dbkvs;

import java.sql.Connection;

import org.immutables.value.Value;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbTableFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SqlConnectionSupplier;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.common.base.Visitors;
import com.palantir.nexus.db.monitoring.timer.SqlTimer;
import com.palantir.nexus.db.monitoring.timer.SqlTimers;
import com.palantir.nexus.db.pool.HikariCPConnectionManager;
import com.palantir.nexus.db.pool.ReentrantManagedConnectionSupplier;
import com.palantir.nexus.db.pool.config.ConnectionConfig;
import com.palantir.nexus.db.sql.SQL;
import com.palantir.nexus.db.sql.SqlConnection;
import com.palantir.nexus.db.sql.SqlConnectionHelper;
import com.palantir.nexus.db.sql.SqlConnectionImpl;

public abstract class DbKeyValueServiceConfig implements KeyValueServiceConfig {

    @Value.Default
    public DbSharedConfig shared() {
        return ImmutableDbSharedConfig.builder().build();
    }

    public abstract ConnectionConfig connection();

    @Value.Derived
    public abstract Supplier<DbTableFactory> tableFactorySupplier();

    @Value.Default
    public Supplier<SqlConnectionSupplier> sqlConnectionSupplier() {
        return () -> createConnectionSupplier();
    }

    private SqlConnectionSupplier createConnectionSupplier() {
        HikariCPConnectionManager connManager = new HikariCPConnectionManager(connection(), Visitors.emptyVisitor());
        ReentrantManagedConnectionSupplier connSupplier = new ReentrantManagedConnectionSupplier(connManager);
        Supplier<Connection> supplier = () -> connSupplier.get();
        SQL sql = new SQL() {
            @Override
            protected SqlConfig getSqlConfig() {
                return new SqlConfig() {
                    @Override
                    public boolean isSqlCancellationDisabled() {
                        return false;
                    }

                    protected Iterable<SqlTimer> getSqlTimers() {
                        return ImmutableList.of(
                                SqlTimers.createDurationSqlTimer(),
                                SqlTimers.createSqlStatsSqlTimer());
                    }

                    @Override
                    final public SqlTimer getSqlTimer() {
                        return SqlTimers.createCombinedSqlTimer(getSqlTimers());
                    }
                };
            }
        };
        return new SqlConnectionSupplier() {
            @Override
            public SqlConnection get() {
                return new SqlConnectionImpl(supplier, new SqlConnectionHelper(sql));
            }

            @Override
            public void close() {
                connSupplier.close();
            }
        };
    }

}
