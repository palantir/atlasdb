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

package com.palantir.timelock.paxos;

import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Optional;
import java.util.function.Function;
import javax.sql.DataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

@SuppressWarnings("FinalClass")
public class PersistedSchemaVersion {
    private static final String ONLY_ROW = "r";
    private final Jdbi jdbi;

    private PersistedSchemaVersion(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public static PersistedSchemaVersion create(DataSource dataSource) {
        Jdbi jdbi = Jdbi.create(dataSource).installPlugin(new SqlObjectPlugin());
        PersistedSchemaVersion schemaVersion = new PersistedSchemaVersion(jdbi);
        schemaVersion.initialize();
        return schemaVersion;
    }

    private void initialize() {
        execute(Queries::createSchemaVersionTable);
    }

    private <T> T execute(Function<Queries, T> call) {
        return jdbi.withExtension(Queries.class, call::apply);
    }

    void upgradeVersion(long targetVersion) {
        execute(dao -> {
            if (dao.getVersion(ONLY_ROW).orElse(0L) < targetVersion) {
                dao.setVersion(ONLY_ROW, targetVersion);
            }
            return null;
        });
    }

    long getVersion() {
        return execute(dao -> dao.getVersion(ONLY_ROW))
                .orElseThrow(() -> new SafeIllegalStateException("No persisted schema version found."));
    }

    public interface Queries {
        @SqlUpdate("CREATE TABLE IF NOT EXISTS schema_version (row TEXT PRIMARY KEY, version BIGINT)")
        boolean createSchemaVersionTable();

        @SqlUpdate("INSERT OR REPLACE INTO schema_version (row, version) VALUES (?, ?)")
        void setVersion(String row, long version);

        @SqlQuery("SELECT version FROM schema_version WHERE row = ?")
        Optional<Long> getVersion(String row);
    }
}
