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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.palantir.common.streams.KeyedStream;

public class SqlitePaxosStateLogTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final Client CLIENT_1 = Client.of("tom");
    private static final Client CLIENT_2 = Client.of("two");

    private static final String SEQUENCE_1 = "seq1";
    private static final String SEQUENCE_2 = "seq2";

    private Supplier<Connection> connSupplier;
    private PaxosStateLog<PaxosValue> stateLog;

    @Before
    public void setup() {
        connSupplier = SqliteConnections
                .createSqliteDatabase(tempFolder.getRoot().toPath().resolve("test.db").toString());
        stateLog = SqlitePaxosStateLog.create(CLIENT_1, SEQUENCE_1, connSupplier);
    }

    @Test
    public void readingNonExistentRoundReturnsNull() throws IOException {
        assertThat(stateLog.readRound(10L)).isNull();
    }

    @Test
    public void canWriteAndRetrieveAValue() throws IOException {
        long round = 12L;
        PaxosValue paxosValue = writeValueForRound(round);
        assertThat(PaxosValue.BYTES_HYDRATOR.hydrateFromBytes(stateLog.readRound(round))).isEqualTo(paxosValue);
    }

    @Test
    public void canWriteAndRetrieveBatch() throws IOException {
        List<PaxosRound<PaxosValue>> inputs = KeyedStream.of(LongStream.rangeClosed(5L, 10L).boxed())
                .map(SqlitePaxosStateLogTest::valueForRound)
                .map((seq, val) -> ImmutablePaxosRound.<PaxosValue>builder().sequence(seq).value(val).build())
                .values()
                .collect(Collectors.toList());
        stateLog.writeBatchOfRounds(inputs);
        for (PaxosRound<PaxosValue> round : inputs) {
            assertThat(PaxosValue.BYTES_HYDRATOR.hydrateFromBytes(stateLog.readRound(round.sequence())))
                    .isEqualTo(round.value());
        }
    }

    @Test
    public void canOverwriteSequences() throws IOException {
        writeValueForRound(5L);
        PaxosValue newEntry = writeValueForRound(5L);
        assertThat(PaxosValue.BYTES_HYDRATOR.hydrateFromBytes(stateLog.readRound(5L))).isEqualTo(newEntry);
    }

    @Test
    public void writesToSameSeqNumberInDifferentSequenceAreDistinct() throws IOException {
        PaxosValue v1 = writeValueForRound(5L);
        PaxosValue v2 = valueForRound(5L);

        PaxosStateLog<PaxosValue> otherLog = SqlitePaxosStateLog.create(CLIENT_2, SEQUENCE_1, connSupplier);
        otherLog.writeRound(5L, v2);

        assertThat(PaxosValue.BYTES_HYDRATOR.hydrateFromBytes(stateLog.readRound(5L))).isEqualTo(v1);
        assertThat(PaxosValue.BYTES_HYDRATOR.hydrateFromBytes(otherLog.readRound(5L))).isEqualTo(v2);
    }

    @Test
    public void returnsDefaultValueForExtremesWhenNoEntries() {
        assertThat(stateLog.getLeastLogEntry()).isEqualTo(PaxosAcceptor.NO_LOG_ENTRY);
        assertThat(stateLog.getGreatestLogEntry()).isEqualTo(PaxosAcceptor.NO_LOG_ENTRY);
    }

    @Test
    public void extremeQueriesIgnoreEntriesFromOtherSequences() {
        PaxosStateLog<PaxosValue> otherLog = SqlitePaxosStateLog.create(CLIENT_2, SEQUENCE_1, connSupplier);
        PaxosStateLog<PaxosValue> anotherLog = SqlitePaxosStateLog.create(CLIENT_1, SEQUENCE_2, connSupplier);
        otherLog.writeRound(1L, valueForRound(1L));
        otherLog.writeRound(5L, valueForRound(5L));
        anotherLog.writeRound(2L, valueForRound(2L));
        anotherLog.writeRound(21L, valueForRound(21L));

        assertThat(stateLog.getLeastLogEntry()).isEqualTo(PaxosAcceptor.NO_LOG_ENTRY);
        assertThat(stateLog.getGreatestLogEntry()).isEqualTo(PaxosAcceptor.NO_LOG_ENTRY);
        assertThat(otherLog.getLeastLogEntry()).isEqualTo(1L);
        assertThat(otherLog.getGreatestLogEntry()).isEqualTo(5L);
        assertThat(anotherLog.getLeastLogEntry()).isEqualTo(2L);
        assertThat(anotherLog.getGreatestLogEntry()).isEqualTo(21L);
    }

    @Test
    public void canGetGreatestLogEntry() {
        writeValueForRound(15L);
        writeValueForRound(19L);
        writeValueForRound(17L);

        assertThat(stateLog.getGreatestLogEntry()).isEqualTo(19L);
    }

    @Test
    public void canGetLeastLogEntry() {
        writeValueForRound(7L);
        writeValueForRound(5L);
        writeValueForRound(9L);

        assertThat(stateLog.getLeastLogEntry()).isEqualTo(5L);
    }

    @Test
    public void canTruncateInclusive() {
        writeValueForRound(5L);
        writeValueForRound(7L);
        writeValueForRound(9L);
        writeValueForRound(1L);

        stateLog.truncate(7L);
        assertThat(stateLog.getLeastLogEntry()).isEqualTo(9L);
    }

    @Test
    public void valuesAreDistinguishedAcrossLogNamespaces() throws IOException {
        PaxosStateLog<PaxosValue> otherLog = SqlitePaxosStateLog.create(CLIENT_2, SEQUENCE_1, connSupplier);
        writeValueForRound(1L);

        assertThat(stateLog.readRound(1L)).isNotNull();
        assertThat(otherLog.readRound(1L)).isNull();
    }

    @Test
    public void valuesAreDistinguishedAcrossSequenceIdentifiers() throws IOException {
        PaxosStateLog<PaxosValue> otherLog = SqlitePaxosStateLog.create(CLIENT_1, SEQUENCE_2, connSupplier);
        writeValueForRound(1L);

        assertThat(stateLog.readRound(1L)).isNotNull();
        assertThat(otherLog.readRound(1L)).isNull();
    }

    @Test
    public void differentLogsToTheSameNamespaceShareState() throws IOException {
        PaxosStateLog<PaxosValue> otherLogWithSameNamespace
                = SqlitePaxosStateLog.create(CLIENT_1, SEQUENCE_1, connSupplier);
        writeValueForRound(1L);

        assertThat(stateLog.readRound(1L)).isNotNull();
        assertThat(otherLogWithSameNamespace.readRound(1L)).isEqualTo(stateLog.readRound(1L));
    }

    private PaxosValue writeValueForRound(long round) {
        PaxosValue paxosValue = valueForRound(round);
        stateLog.writeRound(round, paxosValue);
        return paxosValue;
    }

    private static PaxosValue valueForRound(long round) {
        byte[] bytes = new byte[16];
        ThreadLocalRandom.current().nextBytes(bytes);
        return new PaxosValue("someLeader", round, bytes);
    }
}
