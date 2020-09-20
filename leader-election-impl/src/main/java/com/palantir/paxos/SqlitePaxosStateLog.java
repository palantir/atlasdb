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

import java.util.function.Function;

import javax.sql.DataSource;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.immutables.JdbiImmutables;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import com.palantir.common.persist.Persistable;

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
        jdbi.getConfig(JdbiImmutables.class)
                .registerImmutable(Client.class, PaxosRound.class, NamespaceAndUseCase.class);
        SqlitePaxosStateLog<V> log = new SqlitePaxosStateLog<>(namespaceAndUseCase, jdbi);
        log.initialize();
        return log;
    }

    private void initialize() {
        execute(SqlitePaxosStateLogQueries::createTable);
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

    private <T> T execute(Function<SqlitePaxosStateLogQueries, T> call) {
        return jdbi.withExtension(SqlitePaxosStateLogQueries.class, call::apply);
    }
}
