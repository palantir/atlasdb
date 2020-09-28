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

import java.sql.SQLException;
import java.util.OptionalLong;
import java.util.function.Function;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.keyvalue.dbkvs.OracleErrorConstants;
import com.palantir.nexus.db.pool.ConnectionManager;

public class OraclePhysicalDbBoundStore implements PhysicalDbBoundStore {
    private static final Logger log = LoggerFactory.getLogger(OraclePhysicalDbBoundStore.class);

    private final Jdbi jdbi;
    private final String client;

    private OraclePhysicalDbBoundStore(Jdbi jdbi, String client) {
        this.jdbi = jdbi;
        this.client = client;
    }

    public PhysicalDbBoundStore create(ConnectionManager connectionManager, String client) {
        Jdbi jdbi = Jdbi.create(connectionManager::getConnection)
                .installPlugin(new SqlObjectPlugin());
        return new OraclePhysicalDbBoundStore(jdbi, client);
    }

    @Override
    public void createTimestampTable() {
        try {
            execute(Queries::createTable);
        } catch (Exception e) {
            if (!e.getMessage().contains(OracleErrorConstants.ORACLE_ALREADY_EXISTS_ERROR)) {
                log.error("Error occurred creating the Oracle timestamp table", e);
                throw e;
            }
        }
    }

    @Override
    public OptionalLong read() {
        return execute(queries -> queries.read(client));
    }

    @Override
    public boolean cas(long limit, long previousLimit) {
        return execute(queries -> queries.cas(client, limit, previousLimit));
    }

    @Override
    public boolean initialize(long limit) {
        return execute(queries -> queries.initialize(client, limit));
    }

    private <T> T execute(Function<Queries, T> call) {
        return jdbi.withExtension(Queries.class, call::apply);
    }

    interface Queries {
        @SqlUpdate("CREATE TABLE pt_db_timelock_ts ("
                + "client VARCHAR(2000) NOT NULL,"
                + "last_allocated NUMBER(38) NOT NULL,"
                + "CONSTRAINT pt_db_timelock_ts_pk PRIMARY KEY(client))")
        boolean createTable();

        @SqlQuery("SELECT last_allocated FROM pt_db_timelock_ts WHERE client = :client")
        OptionalLong read(@Bind("client") String client);

        @SqlUpdate("UPDATE pt_db_timelock_ts"
                + " SET client = :client, last_allocated = :limit"
                + " WHERE client = :client AND last_allocated = :previous_limit")
        boolean cas(@Bind("client") String client,
                @Bind("limit") long limit,
                @Bind("previous_limit") long previousLimit);

        @SqlUpdate("INSERT INTO pt_db_timelock_ts (client, last_allocated) VALUES (:client, :limit)")
        boolean initialize(@Bind("client") String client,
                @Bind("limit") long limit);
    }
}
