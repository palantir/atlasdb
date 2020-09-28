/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock;

import java.util.OptionalLong;
import java.util.function.Function;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import com.palantir.nexus.db.pool.ConnectionManager;

public class PostgresPhysicalDbBoundStore implements PhysicalDbBoundStore {
    private final Jdbi jdbi;
    private final String client;

    private PostgresPhysicalDbBoundStore(Jdbi jdbi, String client) {
        this.jdbi = jdbi;
        this.client = client;
    }

    public static PhysicalDbBoundStore create(ConnectionManager connectionManager, String client) {
        Jdbi jdbi = Jdbi.create(connectionManager::getConnection)
                .installPlugin(new SqlObjectPlugin());
        return new PostgresPhysicalDbBoundStore(jdbi, client);
    }

    @Override
    public void createTimestampTable() {
        runTaskTransactionScoped(Queries::createTable);
    }

    @Override
    public OptionalLong read() {
        return runTaskTransactionScoped(queries -> queries.read(client));
    }

    @Override
    public boolean cas(long limit, long previousLimit) {
        return runTaskTransactionScoped(queries -> queries.cas(client, limit, previousLimit));
    }

    @Override
    public boolean initialize(long limit) {
        return runTaskTransactionScoped(queries -> queries.initialize(client, limit));
    }

    private <T> T runTaskTransactionScoped(Function<Queries, T> call) {
        return jdbi.withExtension(Queries.class, call::apply);
    }

    interface Queries {
        @SqlUpdate("CREATE TABLE IF NOT EXISTS pt_db_timelock_ts ("
                + "client VARCHAR(2000) NOT NULL,"
                + "last_allocated int8 NOT NULL,"
                + "PRIMARY KEY (client))")
        boolean createTable();

        @SqlQuery("SELECT last_allocated FROM pt_db_timelock_ts WHERE client = :client")
        OptionalLong read(@Bind("client") String client);

        @SqlUpdate("UPDATE pt_db_timelock_ts"
                + " SET client = :client, last_allocated = :limit"
                + " WHERE client = :client AND last_allocated = :previous_limit")
        boolean cas(@Bind("client") String client,
                @Bind("limit") long limit,
                @Bind("previous_limit") long previousLimit);

        @SqlUpdate("INSERT INTO pt_db_timelock_ts (client, last_allocated) VALUES (:client, :limit)"
                + " ON CONFLICT DO NOTHING")
        boolean initialize(@Bind("client") String client,
                @Bind("limit") long limit);
    }
}
