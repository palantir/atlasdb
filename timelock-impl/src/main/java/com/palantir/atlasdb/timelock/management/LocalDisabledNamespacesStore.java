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

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import javax.sql.DataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

public class LocalDisabledNamespacesStore implements DisabledNamespacesStore {
    private final Jdbi jdbi;

    public LocalDisabledNamespacesStore(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public static LocalDisabledNamespacesStore create(DataSource dataSource) {
        Jdbi jdbi = Jdbi.create(dataSource).installPlugin(new SqlObjectPlugin());
        LocalDisabledNamespacesStore localDisabledNamespacesStore = new LocalDisabledNamespacesStore(jdbi);
        localDisabledNamespacesStore.initialize();
        return localDisabledNamespacesStore;
    }

    private void initialize() {
        execute(Queries::createTable);
    }

    @Override
    public void disable(Set<String> namespaces) {
        namespaces.forEach(this::disable);
    }

    @Override
    public void disable(String namespace) {
        execute(dao -> dao.set(namespace));
    }

    @Override
    public void reEnable(Set<String> namespaces) {
        namespaces.forEach(this::reEnable);
    }

    @Override
    public void reEnable(String namespace) {
        execute(dao -> dao.delete(namespace));
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
