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
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import com.palantir.common.persist.Persistable;

public final class SqlitePaxosStateLog<V extends Persistable & Versionable> implements PaxosStateLog<V> {
    private final Client namespace;
    private final String seqId;
    private final Jdbi jdbi;

    private SqlitePaxosStateLog(Client namespace, String seqId, Jdbi jdbi) {
        this.namespace = namespace;
        this.seqId = seqId;
        this.jdbi = jdbi;
    }

    public static <V extends Persistable & Versionable> PaxosStateLog<V> create(Client namespace,
            String sequenceId,
            Supplier<Connection> connectionSupplier) {
        Jdbi jdbi = Jdbi.create(connectionSupplier::get).installPlugin(new SqlObjectPlugin());
        jdbi.getConfig(JdbiImmutables.class).registerImmutable(Client.class, PaxosRound.class);
        SqlitePaxosStateLog<V> log = new SqlitePaxosStateLog<>(namespace, sequenceId, jdbi);
        log.initialize();
        return log;
    }

    private void initialize() {
        execute(Queries::createTable);
    }

    @Override
    public void writeRound(long seq, V round) {
        execute(dao -> dao.writeRound(namespace, seqId, seq, round.persistToBytes()));
    }

    @Override
    public void writeBatchOfRounds(Iterable<PaxosRound<V>> rounds) {
        execute(dao -> dao.writeBatchOfRounds(namespace, seqId, rounds));
    }

    @Override
    public byte[] readRound(long seq) {
        return execute(dao -> dao.readRound(namespace, seqId, seq));
    }

    @Override
    public long getLeastLogEntry() {
        return execute(dao -> dao.getLeastLogEntry(namespace, seqId)).orElse(PaxosAcceptor.NO_LOG_ENTRY);
    }

    @Override
    public long getGreatestLogEntry() {
        return execute(dao -> dao.getGreatestLogEntry(namespace, seqId)).orElse(PaxosAcceptor.NO_LOG_ENTRY);
    }

    @Override
    public void truncate(long toDeleteInclusive) {
        execute(dao -> dao.truncate(namespace, seqId, toDeleteInclusive));
    }

    private <T> T execute(Function<Queries, T> call) {
        return jdbi.withExtension(Queries.class, call::apply);
    }

    public interface Queries {
        @SqlUpdate("CREATE TABLE IF NOT EXISTS paxosLog ("
                + "namespace TEXT,"
                + "seqId TEXT,"
                + "seq BIGINT, "
                + "val BLOB,"
                + "PRIMARY KEY(namespace, seqId, seq))")
        boolean createTable();

        @SqlUpdate("INSERT OR REPLACE INTO paxosLog (namespace, seqId, seq, val) VALUES ("
                + ":namespace.value, :seqId, :seq, :value)")
        boolean writeRound(
                @BindPojo("namespace") Client namespace,
                @Bind("seqId") String seqId,
                @Bind("seq") long seq,
                @Bind("value") byte[] value);

        @SqlQuery("SELECT val FROM paxosLog WHERE namespace = :namespace.value AND seqId = :seqId AND seq = :seq")
        @SingleValue
        byte[] readRound(@BindPojo("namespace") Client namespace, @Bind("seqId") String seqId, @Bind("seq") long seq);

        @SqlQuery("SELECT MIN(seq) FROM paxosLog WHERE namespace = :namespace.value AND seqId = :seqId")
        OptionalLong getLeastLogEntry(@BindPojo("namespace") Client namespace, @Bind("seqId") String seqId);

        @SqlQuery("SELECT MAX(seq) FROM paxosLog WHERE namespace = :namespace.value AND seqId = :seqId")
        OptionalLong getGreatestLogEntry(@BindPojo("namespace") Client namespace, @Bind("seqId") String seqId);

        @SqlUpdate("DELETE FROM paxosLog WHERE namespace = :namespace.value AND seqId = :seqId AND seq <= :seq")
        boolean truncate(@BindPojo("namespace") Client namespace, @Bind("seqId") String seqId, @Bind("seq") long seq);

        @SqlBatch("INSERT OR REPLACE INTO paxosLog (namespace, seqId, seq, val) VALUES ("
                + ":namespace.value, :seqId, :round.sequence, :round.valueBytes)")
        <V extends Persistable & Versionable> boolean[] writeBatchOfRounds(
                @BindPojo("namespace") Client namespace,
                @Bind("seqId") String seqId,
                @BindPojo("round") Iterable<PaxosRound<V>> rounds);
    }
}
