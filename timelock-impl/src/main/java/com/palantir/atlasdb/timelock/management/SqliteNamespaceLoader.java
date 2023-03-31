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

import com.palantir.paxos.Client;
import com.palantir.paxos.SqlitePaxosStateLog;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import javax.sql.DataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

final class SqliteNamespaceLoader implements PersistentNamespaceLoader {
    private static final String EMPTY_STRING = "";

    private final Jdbi jdbi;

    private SqliteNamespaceLoader(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public static PersistentNamespaceLoader create(DataSource dataSource) {
        Jdbi jdbi = Jdbi.create(dataSource).installPlugin(new SqlObjectPlugin());
        jdbi.withExtension(SqlitePaxosStateLog.Queries.class, SqlitePaxosStateLog.Queries::createTable);
        return new SqliteNamespaceLoader(jdbi);
    }

    @Override
    public Set<Client> getAllPersistedNamespaces() {
        Set<Client> clients = new HashSet<>();

        // Namespaces contain at least one character implying the empty string is lexicographically strictly smaller
        // than any namespace.
        Optional<String> currentNamespace = getNextLexicographicallySmallestNamespace(EMPTY_STRING);
        while (currentNamespace.isPresent()) {
            String namespaceString = currentNamespace.get();
            clients.add(Client.of(namespaceString));
            currentNamespace = getNextLexicographicallySmallestNamespace(namespaceString);
        }

        return clients;
    }

    private Optional<String> getNextLexicographicallySmallestNamespace(String maybeLastReadNamespace) {
        return jdbi.withExtension(
                SqlitePaxosStateLog.Queries.class,
                dao -> dao.getNextLexicographicallySmallestNamespace(maybeLastReadNamespace));
    }
}
