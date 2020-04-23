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
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Suppliers;
import com.google.common.reflect.AbstractInvocationHandler;
import com.palantir.common.streams.KeyedStream;

public class SqlitePaxosStateLogTest {
    private static final String LOG_NAMESPACE_1 = "tom";
    private static final String LOG_NAMESPACE_2 = "two";

    private Supplier<Connection> connSupplier;
    private SqlitePaxosStateLog<PaxosValue> stateLog;

    @Before
    public void setup() {
        connSupplier = createReusableMemoizedConnection();
        stateLog = SqlitePaxosStateLog.create(LOG_NAMESPACE_1, connSupplier);
    }

    @Test
    public void readingNonExistentRoundReturnsNull() {
        assertThat(stateLog.readRound(10L)).isNull();
    }

    @Test
    public void canWriteAndRetrieveAValue() {
        long round = 12L;
        PaxosValue paxosValue = writeValueForRound(round);
        assertThat(PaxosValue.BYTES_HYDRATOR.hydrateFromBytes(stateLog.readRound(round))).isEqualTo(paxosValue);
    }

    @Test
    public void canWriteAndRetrieveBatch() {
        List<PaxosRound<PaxosValue>> inputs = KeyedStream.of(
                LongStream.rangeClosed(5L, 10L).boxed())
                .map(SqlitePaxosStateLogTest::valueForRound)
                .map((a, b) -> ImmutablePaxosRound.<PaxosValue>builder().sequence(a).value(b).build()).values()
                .collect(Collectors.toList());
        stateLog.writeBatchOfRounds(inputs);
        inputs.forEach(round ->
                assertThat(PaxosValue.BYTES_HYDRATOR.hydrateFromBytes(stateLog.readRound(round.sequence())))
                        .isEqualTo(round.value()));
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

    @Test
    public void valuesAreDistinguishedAcrossLogNamespaces() throws IOException {
        PaxosStateLog<PaxosValue> otherLog = SqlitePaxosStateLog.create(LOG_NAMESPACE_2, connSupplier);
        writeValueForRound(1L);

        assertThat(stateLog.readRound(1L)).isNotNull();
        assertThat(otherLog.readRound(1L)).isNull();
    }

    @Test
    public void differentLogsToTheSameNamespaceShareState() throws IOException {
        PaxosStateLog<PaxosValue> otherLogWithSameNamespace = SqlitePaxosStateLog.create(LOG_NAMESPACE_1, connSupplier);
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

    private static Supplier<Connection> createReusableMemoizedConnection() {
        Supplier<Connection> baseConnectionSupplier = SqliteConnections.createDatabaseForTest();
        Supplier<Connection> nonClosingConnectionSupplier = bypassCloseOnConnection(baseConnectionSupplier);
        return Suppliers.memoize(nonClosingConnectionSupplier::get);
    }

    private static Supplier<Connection> bypassCloseOnConnection(Supplier<Connection> connectionSupplier) {
        return Suppliers.compose(conn ->
                        (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                                new Class<?>[] {Connection.class},
                                new CloseIgnoringInvocationHandler(conn)),
                connectionSupplier::get);
    }

    private static final class CloseIgnoringInvocationHandler extends AbstractInvocationHandler {
        private final Connection delegate;

        private CloseIgnoringInvocationHandler(Connection delegate) {
            this.delegate = delegate;
        }

        @Override
        protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getName().equals("close")) {
                return null;
            }
            return method.invoke(delegate, args);
        }
    }
}
