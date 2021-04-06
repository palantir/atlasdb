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

package com.palantir.timelock.store;

import java.io.InputStream;
import java.util.Optional;
import java.util.function.Function;
import javax.sql.DataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

public final class SqliteBlobStore {
    private final Jdbi jdbi;

    private SqliteBlobStore(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public static SqliteBlobStore create(DataSource dataSource) {
        Jdbi jdbi = Jdbi.create(dataSource)
                .installPlugin(new SqlObjectPlugin())
                .registerColumnMapper(InputStream.class, (rs, columnNumber, ctx) -> rs.getBinaryStream(columnNumber));
        SqliteBlobStore blobStore = new SqliteBlobStore(jdbi);
        blobStore.initialize();
        return blobStore;
    }

    private void initialize() {
        execute(Queries::createBlobStoreTable);
    }

    private <T> T execute(Function<SqliteBlobStore.Queries, T> call) {
        return jdbi.withExtension(SqliteBlobStore.Queries.class, call::apply);
    }

    Optional<byte[]> getValue(BlobStoreUseCase useCase) {
        return execute(dao -> dao.getVersion(useCase.getShortName()));
    }

    void putValue(BlobStoreUseCase useCase, byte[] blob) {
        execute(dao -> {
            dao.putBlob(useCase.getShortName(), blob);
            return null;
        });
    }

    public interface Queries {
        @SqlUpdate("CREATE TABLE IF NOT EXISTS blob_store (row TEXT PRIMARY KEY, value BLOB)")
        boolean createBlobStoreTable();

        @SqlUpdate("INSERT OR REPLACE INTO blob_store (row, value) VALUES (?, ?)")
        void putBlob(@Bind String row, @Bind byte[] blob);

        @SqlQuery("SELECT value FROM blob_store WHERE row = ?")
        Optional<byte[]> getVersion(@Bind String row);
    }
}
