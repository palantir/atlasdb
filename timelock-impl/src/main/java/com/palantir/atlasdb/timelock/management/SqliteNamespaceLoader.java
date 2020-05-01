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

package com.palantir.atlasdb.timelock.management;

import java.sql.Connection;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import com.palantir.paxos.Client;
import com.palantir.paxos.SqlitePaxosStateLog;

public final class SqliteNamespaceLoader {
    private final Jdbi jdbi;

    private SqliteNamespaceLoader(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public static SqliteNamespaceLoader create(Supplier<Connection> connectionSupplier) {
        Jdbi jdbi = Jdbi.create(connectionSupplier::get).installPlugin(new SqlObjectPlugin());
        jdbi.withExtension(SqlitePaxosStateLog.Queries.class, SqlitePaxosStateLog.Queries::createTable);
        return new SqliteNamespaceLoader(jdbi);
    }

    public Set<Client> getAllRegisteredNamespaces() {
        return jdbi.withExtension(SqlitePaxosStateLog.Queries.class, SqlitePaxosStateLog.Queries::getAllNamespaces)
                .stream()
                .map(Client::of)
                .collect(Collectors.toSet());
    }
}
