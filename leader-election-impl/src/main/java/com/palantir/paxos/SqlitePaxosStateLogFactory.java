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

package com.palantir.paxos;

import java.sql.Connection;
import java.util.List;
import java.util.function.Supplier;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import com.palantir.common.persist.Persistable;

public class SqlitePaxosStateLogFactory<V extends Persistable & Versionable> {
    public static final String NAMESPACE_TABLE = "_paxosNamespaces";

    private final Supplier<Connection> connectionSupplier;
    private final Jdbi jdbi;

    private SqlitePaxosStateLogFactory(Supplier<Connection> connectionSupplier, Jdbi jdbi) {
        this.connectionSupplier = connectionSupplier;
        this.jdbi = jdbi;
    }

    public static <V extends Persistable & Versionable> SqlitePaxosStateLogFactory<V> create(
            Supplier<Connection> connectionSupplier) {
        Jdbi jdbi = Jdbi.create(connectionSupplier::get).installPlugin(new SqlObjectPlugin());
        jdbi.withExtension(Queries.class, Queries::createNamespaceTable);
        return new SqlitePaxosStateLogFactory<>(connectionSupplier, jdbi);
    }

    public PaxosStateLog<V> createPaxosStateLog(String namespace) {
        jdbi.withExtension(Queries.class, queries -> queries.registerNamespace(namespace));
        return SqlitePaxosStateLog.create(namespace, connectionSupplier);
    }

    public List<String> getAllRegisteredNamespaces() {
        return jdbi.withExtension(Queries.class, Queries::getAllRegisteredNamespaces);
    }

    public interface Queries {
        @SqlUpdate("CREATE TABLE IF NOT EXISTS " + NAMESPACE_TABLE + " (namespace TEXT PRIMARY KEY)")
        boolean createNamespaceTable();

        @SqlUpdate("INSERT OR REPLACE INTO " + NAMESPACE_TABLE + " VALUES (:namespace)")
        boolean registerNamespace(@Bind("namespace") String namespace);

        @SqlQuery("SELECT namespace FROM " + NAMESPACE_TABLE)
        List<String> getAllRegisteredNamespaces();
    }
}
