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
import java.util.OptionalLong;
import java.util.function.Function;
import java.util.function.Supplier;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.immutables.JdbiImmutables;
import org.jdbi.v3.sqlobject.SingleValue;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindPojo;
import org.jdbi.v3.sqlobject.customizer.Define;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import com.palantir.common.persist.Persistable;

public final class SqlitePaxosStateLog<V extends Persistable & Versionable> implements PaxosStateLog<V> {
    private final String namespace;
    private final Jdbi jdbi;

    private SqlitePaxosStateLog(String namespace, Jdbi jdbi) {
        this.namespace = namespace;
        this.jdbi = jdbi;
    }

    public static <V extends Persistable & Versionable> SqlitePaxosStateLog<V> create(String namespace,
            Supplier<Connection> connectionSupplier) {
        Jdbi jdbi = Jdbi.create(connectionSupplier::get).installPlugin(new SqlObjectPlugin());
        jdbi.getConfig(JdbiImmutables.class).registerImmutable(PaxosRound.class);
        SqlitePaxosStateLog<V> log = new SqlitePaxosStateLog<>(namespace, jdbi);
        log.initialize();
        return log;
    }

    private void initialize() {
        execute(dao -> dao.createTable(namespace));
    }

    @Override
    public void writeRound(long seq, V round) {
        execute(dao -> dao.writeRound(namespace, seq, round.persistToBytes()));
    }

    @Override
    public void writeBatchOfRounds(Iterable<PaxosRound<V>> rounds) {
        execute(dao -> dao.writeBatchOfRounds(namespace, rounds));
    }

    @Override
    public byte[] readRound(long seq) {
        return execute(dao -> dao.readRound(namespace, seq));
    }

    @Override
    public long getLeastLogEntry() {
        return execute(dao -> dao.getLeastLogEntry(namespace)).orElse(PaxosAcceptor.NO_LOG_ENTRY);
    }

    @Override
    public long getGreatestLogEntry() {
        return execute(dao -> dao.getGreatestLogEntry(namespace)).orElse(PaxosAcceptor.NO_LOG_ENTRY);
    }

    @Override
    public void truncate(long toDeleteInclusive) {
        execute(dao -> dao.truncate(namespace, toDeleteInclusive));
    }

    public void reinitialize() {
        execute(dao -> {
            dao.dropTable(namespace);
            dao.createTable(namespace);
            return null;
        });
    }

    private <T> T execute(Function<Queries, T> call) {
        return jdbi.withExtension(Queries.class, call::apply);
    }

    public interface Queries {
        @SqlUpdate("CREATE TABLE IF NOT EXISTS <table> (seq BIGINT PRIMARY KEY, val BLOB)")
        boolean createTable(@Define("table") String table);

        @SqlUpdate("DROP TABLE <table>")
        boolean dropTable(@Define("table") String table);

        @SqlUpdate("INSERT OR REPLACE INTO <table> (seq, val) VALUES (:seq, :value)")
        boolean writeRound(@Define("table") String table, @Bind("seq") long seq, @Bind("value") byte[] value);

        @SqlQuery("SELECT val FROM <table> WHERE seq = :seq")
        @SingleValue
        byte[] readRound(@Define("table") String table, @Bind("seq") long seq);

        @SqlQuery("SELECT MIN(seq) FROM <table>")
        OptionalLong getLeastLogEntry(@Define("table") String table);

        @SqlQuery("SELECT MAX(seq) FROM <table>")
        OptionalLong getGreatestLogEntry(@Define("table") String table);

        @SqlUpdate("DELETE FROM <table> WHERE seq <= :seq")
        boolean truncate(@Define("table") String table, @Bind("seq") long seq);

        @SqlBatch("INSERT OR REPLACE INTO <table> (seq, val) VALUES (:round.sequence, :round.valueBytes)")
        <V extends Persistable & Versionable> boolean[] writeBatchOfRounds(@Define("table") String table,
                @BindPojo("round") Iterable<PaxosRound<V>> rounds);
    }
}
