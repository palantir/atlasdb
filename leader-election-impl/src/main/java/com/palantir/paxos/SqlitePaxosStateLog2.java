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
import java.util.Collection;
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

public final class SqlitePaxosStateLog2<V extends Persistable & Versionable> implements PaxosStateLog<V> {
    private final String namespace;
    private final Jdbi jdbi;

    private SqlitePaxosStateLog2(String namespace, Jdbi jdbi) {
        this.namespace = namespace;
        this.jdbi = jdbi;
    }

    public static <V extends Persistable & Versionable> SqlitePaxosStateLog2<V> create(String namespace,
            Supplier<Connection> connectionSupplier) {
        Jdbi jdbi = Jdbi.create(connectionSupplier::get).installPlugin(new SqlObjectPlugin());
        jdbi.getConfig(JdbiImmutables.class).registerImmutable(PaxosRound.class);
        SqlitePaxosStateLog2<V> log = new SqlitePaxosStateLog2<>(namespace, jdbi);
        log.initialize();
        return log;
    }

    private void initialize() {
        execute(dao -> dao.createTable());
    }

    @Override
    public void writeRound(long seq, V round) {
        execute(dao -> dao.writeRound(seq, namespace, round.persistToBytes()));
    }

    @Override
    public void writeBatchOfRounds(Collection<PaxosRound<V>> rounds) {
        execute(dao -> dao.writeBatchOfRounds(rounds, namespace));
    }

    @Override
    public byte[] readRound(long seq) {
        return execute(dao -> dao.readRound(seq, namespace));
    }

    @Override
    public long getLeastLogEntry() {
        return execute(x -> x.getLeastLogEntry(namespace)).orElse(PaxosAcceptor.NO_LOG_ENTRY);
    }

    @Override
    public long getGreatestLogEntry() {
        return execute(x -> x.getGreatestLogEntry(namespace)).orElse(PaxosAcceptor.NO_LOG_ENTRY);
    }

    @Override
    public void truncate(long toDeleteInclusive) {
        execute(dao -> dao.truncate(toDeleteInclusive, namespace));
    }

    private <T> T execute(Function<Queries, T> call) {
        return jdbi.withExtension(Queries.class, call::apply);
    }

    public interface Queries {
        @SqlUpdate("CREATE TABLE IF NOT EXISTS tab (seq BIGINT, sec TEXT, val BLOB, PRIMARY KEY (sec, seq))")
        boolean createTable();

        @SqlUpdate("INSERT OR REPLACE INTO tab (seq, val) VALUES (:seq, :sec, :value)")
        boolean writeRound(@Bind("seq") long seq, @Bind("sec") String sec, @Bind("value") byte[] value);

        @SqlQuery("SELECT val FROM tab WHERE seq = :seq AND sec = :sec")
        @SingleValue
        byte[] readRound(@Bind("seq") long seq, @Bind("sec") String sec);

        @SqlQuery("SELECT MIN(seq) FROM tab WHERE sec = :sec")
        OptionalLong getLeastLogEntry(@Bind("sec") String sec);

        @SqlQuery("SELECT MAX(seq) FROM tab WHERE sec = :sec")
        OptionalLong getGreatestLogEntry(@Bind("sec") String sec);

        @SqlUpdate("DELETE FROM tab WHERE seq <= :seq AND sec = :sec")
        boolean truncate(@Bind("seq") long seq, @Bind("sec") String sec);

        @SqlBatch("INSERT OR REPLACE INTO tab (seq, sec, val) VALUES (:round.sequence, :sec, :round.valueBytes)")
        <V extends Persistable & Versionable> boolean[] writeBatchOfRounds(
                @BindPojo("round") Collection<PaxosRound<V>> rounds, @Bind("sec") String sec);
    }
}
