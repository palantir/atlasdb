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

import com.palantir.common.persist.Persistable;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import javax.sql.DataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.immutables.JdbiImmutables;
import org.jdbi.v3.sqlobject.SingleValue;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindPojo;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

@SuppressWarnings("checkstyle:FinalClass") // non-final for mocking
public class SqlitePaxosStateLog<V extends Persistable & Versionable> implements PaxosStateLog<V> {
    private final Client namespace;
    private final String useCase;
    private final Jdbi jdbi;

    private SqlitePaxosStateLog(NamespaceAndUseCase namespaceAndUseCase, Jdbi jdbi) {
        this.namespace = namespaceAndUseCase.namespace();
        this.useCase = namespaceAndUseCase.useCase();
        this.jdbi = jdbi;
    }

    public static <V extends Persistable & Versionable> PaxosStateLog<V> create(
            NamespaceAndUseCase namespaceAndUseCase,
            DataSource dataSource) {
        Jdbi jdbi = Jdbi.create(dataSource).installPlugin(new SqlObjectPlugin());
        jdbi.getConfig(JdbiImmutables.class).registerImmutable(Client.class, PaxosRound.class);
        SqlitePaxosStateLog<V> log = new SqlitePaxosStateLog<>(namespaceAndUseCase, jdbi);
        log.initialize();
        return log;
    }

    private void initialize() {
        execute(Queries::createTable);
    }

    @Override
    public void writeRound(long seq, V round) {
        execute(dao -> dao.writeRound(namespace, useCase, seq, round.persistToBytes()));
    }

    @Override
    public void writeBatchOfRounds(Iterable<PaxosRound<V>> rounds) {
        execute(dao -> dao.writeBatchOfRounds(namespace, useCase, rounds));
    }

    @Override
    public byte[] readRound(long seq) {
        return execute(dao -> dao.readRound(namespace, useCase, seq));
    }

    @Override
    public long getLeastLogEntry() {
        return execute(dao -> dao.getLeastLogEntry(namespace, useCase)).orElse(PaxosAcceptor.NO_LOG_ENTRY);
    }

    @Override
    public long getGreatestLogEntry() {
        return execute(dao -> dao.getGreatestLogEntry(namespace, useCase)).orElse(PaxosAcceptor.NO_LOG_ENTRY);
    }

    @Override
    public void truncate(long toDeleteInclusive) {
        execute(dao -> dao.truncate(namespace, useCase, toDeleteInclusive));
    }

    private <T> T execute(Function<Queries, T> call) {
        return jdbi.withExtension(Queries.class, call::apply);
    }

    public interface Queries {
        @SqlUpdate("CREATE TABLE IF NOT EXISTS paxosLog ("
                + "namespace TEXT,"
                + "useCase TEXT,"
                + "seq BIGINT,"
                + "val BLOB,"
                + "PRIMARY KEY(namespace, useCase, seq))")
        boolean createTable();

        @SqlUpdate("INSERT OR REPLACE INTO paxosLog (namespace, useCase, seq, val) VALUES ("
                + ":namespace.value, :useCase, :seq, :value)")
        boolean writeRound(
                @BindPojo("namespace") Client namespace,
                @Bind("useCase") String useCase,
                @Bind("seq") long seq,
                @Bind("value") byte[] value);

        @SqlQuery("SELECT val FROM paxosLog WHERE namespace = :namespace.value AND useCase = :useCase AND seq = :seq")
        @SingleValue
        byte[] readRound(
                @BindPojo("namespace") Client namespace,
                @Bind("useCase") String useCase,
                @Bind("seq") long seq);

        @SqlQuery("SELECT MIN(seq) FROM paxosLog WHERE namespace = :namespace.value AND useCase = :useCase")
        OptionalLong getLeastLogEntry(@BindPojo("namespace") Client namespace, @Bind("useCase") String useCase);

        @SqlQuery("SELECT MAX(seq) FROM paxosLog WHERE namespace = :namespace.value AND useCase = :useCase")
        OptionalLong getGreatestLogEntry(@BindPojo("namespace") Client namespace, @Bind("useCase") String useCase);

        @SqlUpdate("DELETE FROM paxosLog WHERE namespace = :namespace.value AND useCase = :useCase AND seq <= :seq")
        boolean truncate(
                @BindPojo("namespace") Client namespace,
                @Bind("useCase") String useCase,
                @Bind("seq") long seq);

        @SqlBatch("INSERT OR REPLACE INTO paxosLog (namespace, useCase, seq, val) VALUES ("
                + ":namespace.value, :useCase, :round.sequence, :round.valueBytes)")
        <V extends Persistable & Versionable> boolean[] writeBatchOfRounds(
                @BindPojo("namespace") Client namespace,
                @Bind("useCase") String useCase,
                @BindPojo("round") Iterable<PaxosRound<V>> rounds);

        @SqlQuery("SELECT DISTINCT(namespace) FROM paxosLog")
        Set<String> getAllNamespaces();
    }
}
