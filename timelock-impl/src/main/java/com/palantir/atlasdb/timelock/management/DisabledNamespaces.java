/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.atlasdb.timelock.api.Namespace;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import javax.sql.DataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

public class DisabledNamespaces {
    private final Jdbi jdbi;

    public DisabledNamespaces(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public static DisabledNamespaces create(DataSource dataSource) {
        Jdbi jdbi = Jdbi.create(dataSource).installPlugin(new SqlObjectPlugin());
        DisabledNamespaces disabledNamespaces = new DisabledNamespaces(jdbi);
        disabledNamespaces.initialize();
        return disabledNamespaces;
    }

    private void initialize() {
        execute(Queries::createTable);
    }

    public void disable(Namespace namespace) {
        execute(dao -> dao.set(namespace.get()));
    }

    public void disable(String namespace) {
        execute(dao -> dao.set(namespace));
    }

    public boolean isDisabled(String namespace) {
        return execute(dao -> dao.getState(namespace)).isPresent();
    }

    public boolean isEnabled(String namespace) {
        return execute(dao -> dao.getState(namespace)).isEmpty();
    }

    public Set<String> disabledNamespaces() {
        return execute(Queries::getAllStates);
    }

    public void reEnable(Namespace namespace) {
        execute(dao -> dao.delete(namespace.get()));
    }

    public void reEnable(String namespace) {
        execute(dao -> dao.delete(namespace));
    }

    private <T> T execute(Function<Queries, T> call) {
        return jdbi.withExtension(Queries.class, call::apply);
    }

    public interface Queries {
        @SqlUpdate("CREATE TABLE IF NOT EXISTS disabled (namespace TEXT PRIMARY KEY)")
        boolean createTable();

        @SqlUpdate("INSERT OR REPLACE INTO disabled (namespace) VALUES (?)")
        boolean set(String namespace);

        @SqlQuery("SELECT namespace FROM disabled WHERE namespace = ?")
        Optional<String> getState(String namespace);

        @SqlQuery("SELECT namespace FROM disabled")
        Set<String> getAllStates();

        @SqlUpdate("DELETE FROM disabled WHERE namespace = ?")
        boolean delete(String namespace);
    }
}
