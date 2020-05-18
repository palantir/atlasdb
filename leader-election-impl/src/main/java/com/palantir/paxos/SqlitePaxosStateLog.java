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
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
    private final String useCase;
    private final Jdbi jdbi;
    private final ReadWriteLock sharedLock;

    private SqlitePaxosStateLog(NamespaceAndUseCase namespaceAndUseCase, Jdbi jdbi, ReadWriteLock sharedLock) {
        this.namespace = namespaceAndUseCase.namespace();
        this.useCase = namespaceAndUseCase.useCase();
        this.jdbi = jdbi;
        this.sharedLock = sharedLock;
    }

    private static <V extends Persistable & Versionable> PaxosStateLog<V> create(
            NamespaceAndUseCase namespaceAndUseCase,
            Supplier<Connection> connectionSupplier, ReadWriteLock sharedLock) {
        Jdbi jdbi = Jdbi.create(connectionSupplier::get).installPlugin(new SqlObjectPlugin());
        jdbi.getConfig(JdbiImmutables.class).registerImmutable(Client.class, PaxosRound.class);
        SqlitePaxosStateLog<V> log = new SqlitePaxosStateLog<>(namespaceAndUseCase, jdbi, sharedLock);
        log.initialize();
        return log;
    }

    public static SqlitePaxosStateLogFactory createFactory() {
        return new SqlitePaxosStateLogFactory();
    }

    private void initialize() {
        executeWrite(Queries::createTable);
    }

    @Override
    public void writeRound(long seq, V round) {
        executeWrite(dao -> dao.writeRound(namespace, useCase, seq, round.persistToBytes()));
    }

    @Override
    public void writeBatchOfRounds(Iterable<PaxosRound<V>> rounds) {
        executeWrite(dao -> dao.writeBatchOfRounds(namespace, useCase, rounds));
    }

    @Override
    public byte[] readRound(long seq) {
        return executeRead(dao -> dao.readRound(namespace, useCase, seq));
    }

    @Override
    public long getLeastLogEntry() {
        return executeRead(dao -> dao.getLeastLogEntry(namespace, useCase)).orElse(PaxosAcceptor.NO_LOG_ENTRY);
    }

    @Override
    public long getGreatestLogEntry() {
        return executeRead(dao -> dao.getGreatestLogEntry(namespace, useCase)).orElse(PaxosAcceptor.NO_LOG_ENTRY);
    }

    @Override
    public void truncate(long toDeleteInclusive) {
        executeWrite(dao -> dao.truncate(namespace, useCase, toDeleteInclusive));
    }

    private <T> T executeWrite(Function<Queries, T> call) {
        sharedLock.writeLock().lock();
        try {
            return execute(call);
        } finally {
            sharedLock.writeLock().unlock();
        }
    }

    private <T> T executeRead(Function<Queries, T> call) {
        sharedLock.readLock().lock();
        try {
            return execute(call);
        } finally {
            sharedLock.readLock().unlock();
        }
    }

    private <T> T execute(Function<Queries, T> call) {
        return jdbi.withExtension(Queries.class, call::apply);
    }

    public static class SqlitePaxosStateLogFactory {
        private final ReadWriteLock sharedLock = new ReentrantReadWriteLock();

        public <V extends Persistable & Versionable> PaxosStateLog<V> create(
                NamespaceAndUseCase namespaceAndUseCase,
                Supplier<Connection> connectionSupplier) {
            return SqlitePaxosStateLog.create(namespaceAndUseCase, connectionSupplier, sharedLock);
        }
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
