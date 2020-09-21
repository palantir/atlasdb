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

package com.palantir.history;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.immutables.JdbiImmutables;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.palantir.history.mappers.LearnerPaxosRoundMapper;
import com.palantir.history.mappers.NamespaceAndUseCaseMapper;
import com.palantir.paxos.Client;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.paxos.PaxosRound;
import com.palantir.paxos.PaxosStateLog;
import com.palantir.paxos.PaxosValue;
import com.palantir.paxos.SqliteConnections;
import com.palantir.paxos.SqlitePaxosStateLog;
import com.palantir.paxos.SqlitePaxosStateLogQueries;

public class SqliteHistoryQueryTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final Client CLIENT = Client.of("tom");
    private static final String USE_CASE = "useCase1";

    private DataSource dataSource;
    private Jdbi jdbi;
    private PaxosStateLog<PaxosValue> stateLog;

    @Before
    public void setup() {
        dataSource = SqliteConnections.getPooledDataSource(tempFolder.getRoot().toPath());
        stateLog = SqlitePaxosStateLog.create(ImmutableNamespaceAndUseCase.of(CLIENT, USE_CASE), dataSource);

        jdbi = Jdbi.create(dataSource).installPlugin(new SqlObjectPlugin());
        jdbi.getConfig(JdbiImmutables.class).registerImmutable(Client.class, PaxosRound.class);
        jdbi.registerRowMapper(new LearnerPaxosRoundMapper());
        jdbi.registerRowMapper(new NamespaceAndUseCaseMapper());
    }

    @Test
    public void canGetAllLogsSince() {
        IntStream.range(0, 100).forEach(i -> writeValueForLogAndRound(stateLog, i + 1));
        Function<SqlitePaxosStateLogQueries, Set<PaxosRound<PaxosValue>>> call
                = dao -> dao.getLearnerLogsSince(CLIENT, USE_CASE, 5L);
        Set<PaxosRound<PaxosValue>> namespaceAndUseCases = jdbi.withExtension(SqlitePaxosStateLogQueries.class,
                call::apply);
        assertThat(namespaceAndUseCases.size()).isEqualTo(95);
    }

    @Test
    public void canGetAllUniquePairsOfNamespaceAndClient() {
        IntStream.range(0, 100).forEach(i -> {
            PaxosStateLog<PaxosValue> otherLog
                    = SqlitePaxosStateLog.create(ImmutableNamespaceAndUseCase.of(Client.of("client" + i), USE_CASE), dataSource);
            writeValueForLogAndRound(otherLog, 1L);
        });
        Set<NamespaceAndUseCase> namespaceAndUseCases = jdbi.withExtension(SqlitePaxosStateLogQueries.class,
                SqlitePaxosStateLogQueries::getAllNamespaceAndUseCaseTuples);
        assertThat(namespaceAndUseCases.size()).isEqualTo(100);
    }

    private PaxosValue writeValueForLogAndRound(PaxosStateLog<PaxosValue> log, long round) {
        PaxosValue paxosValue = new PaxosValue("leaderUuid", round, longToBytes(round));
        log.writeRound(round, paxosValue);
        return paxosValue;
    }

    private byte[] longToBytes(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        return buffer.array();
    }
}
