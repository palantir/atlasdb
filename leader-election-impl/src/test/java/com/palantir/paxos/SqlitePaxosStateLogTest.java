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
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Suppliers;

public class SqlitePaxosStateLogTest {
    private PaxosStateLog<PaxosValue> stateLog;

    @Before
    public void setup() {
        Supplier<Connection> connectionSupplier = Suppliers.memoize(SqliteConnections.createDatabaseForTest()::get);
        stateLog =  SqlitePaxosStateLog.createInitialized(connectionSupplier);
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
    public void canOverwriteSequences() throws IOException {
        writeValueForRound(5L);
        PaxosValue newEntry = writeValueForRound(5L);
        assertThat(PaxosValue.BYTES_HYDRATOR.hydrateFromBytes(stateLog.readRound(5L))).isEqualTo(newEntry);
    }

    @Test
    public void returnsDefaultValueForExtremesWhenNoEntries() {
        assertThat(stateLog.getLeastLogEntry()).isEqualTo(PaxosAcceptor.NO_LOG_ENTRY);
        assertThat(stateLog.getGreatestLogEntry()).isEqualTo(PaxosAcceptor.NO_LOG_ENTRY);
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
