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
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.junit.Test;

import com.palantir.common.streams.KeyedStream;

public class SqliteBenchmarkThrowaway {
    private Supplier<Connection> connectionSupplier = SqliteConnections.createOnDisk();
    private PaxosStateLog<PaxosValue> stateLog = SqlitePaxosStateLog.create("test", connectionSupplier);




    @Test
    public void test() throws IOException {
        for(int i = 0; i < 100; i++) {
            List<PaxosRound<PaxosValue>> inputs = KeyedStream.of(
                    LongStream.range(i * 10000, (i + 1) * 10000).boxed())
                    .map(SqliteBenchmarkThrowaway::valueForRound)
                    .map((a, b) -> ImmutablePaxosRound.<PaxosValue>builder().sequence(a).value(b).build()).values()
                    .collect(Collectors.toList());
            stateLog.writeBatchOfRounds(inputs);
        }
        Instant startTime = Instant.now();
        stateLog.getLeastLogEntry();
        System.out.println(Duration.between(startTime, Instant.now()));
        startTime = Instant.now();
        for (int i = 0; i < 100; i++) {
            stateLog.getLeastLogEntry();
        }
        System.out.println(Duration.between(startTime, Instant.now()));
        startTime = Instant.now();
        stateLog.getGreatestLogEntry();
        System.out.println(Duration.between(startTime, Instant.now()));
        startTime = Instant.now();
        for (int i = 0; i < 100; i++) {
            stateLog.getGreatestLogEntry();
        }
        System.out.println(Duration.between(startTime, Instant.now()));
        startTime = Instant.now();
        for (long i = 990_000; i < 1_000_000; i++) {
            stateLog.readRound(i);
        }
        System.out.println(Duration.between(startTime, Instant.now()));
    }


    /**
     * one table, additional columns 1M * 5 entries
     * PT0.015S
     * PT0.165S
     * PT0.002S
     * PT0.139S
     * PT8.952S
     *
     * PT0.016S
     * PT0.188S
     * PT0.002S
     * PT0.146S
     * PT9.704S
     *
     * one table, additional columns 50M entries
     * PT0.013S
     * PT0.145S
     * PT0.002S
     * PT0.122S
     * PT9.304S
     *
     * 5 tables, each 1M entries
     * PT0.014S
     * PT0.158S
     * PT0.002S
     * PT0.171S
     * PT9.8S
     *
     * PT0.022S
     * PT0.203S
     * PT0.004S
     * PT0.167S
     * PT11.119S
     *
     * 5 tables, each 10M entries
     * PT0.015S
     * PT0.148S
     * PT0.002S
     * PT0.126S
     * PT9.417S
     * @throws IOException
     */
    @Test
    public void test2() throws IOException {
        List<PaxosStateLog<PaxosValue>> logs = Stream.of("gera", "ghretwh", "erthw", "btwrhb", "bnrtwhnw")
                .map(x -> SqlitePaxosStateLog2.<PaxosValue>create(x, connectionSupplier))
                .collect(Collectors.toList());

        for(int i = 0; i < 200; i++) {
            List<PaxosRound<PaxosValue>> inputs = KeyedStream.of(
                    LongStream.range(i * 50000, (i + 1) * 50000).boxed())
                    .map(SqliteBenchmarkThrowaway::valueForRound)
                    .map((a, b) -> ImmutablePaxosRound.<PaxosValue>builder().sequence(a).value(b).build()).values()
                    .collect(Collectors.toList());
            logs.forEach(log -> log.writeBatchOfRounds(inputs));
        }

        Instant startTime = Instant.now();
        stateLog.getLeastLogEntry();
        System.out.println(Duration.between(startTime, Instant.now()));
        startTime = Instant.now();
        for (int i = 0; i < 100; i++) {
            logs.get(i % 5).getLeastLogEntry();
        }
        System.out.println(Duration.between(startTime, Instant.now()));
        startTime = Instant.now();
        stateLog.getGreatestLogEntry();
        System.out.println(Duration.between(startTime, Instant.now()));
        startTime = Instant.now();
        for (int i = 0; i < 100; i++) {
            logs.get(i % 5).getGreatestLogEntry();
        }
        System.out.println(Duration.between(startTime, Instant.now()));
        startTime = Instant.now();
        for (long i = 9_990_000; i < 10_000_000; i++) {
            logs.get((int) i % 5).readRound(i);
        }
        System.out.println(Duration.between(startTime, Instant.now()));
    }

    @Test
    public void canWriteAndRetrieveAValue() throws IOException {
        long round = 12L;
        PaxosValue paxosValue = writeValueForRound(round);
        assertThat(PaxosValue.BYTES_HYDRATOR.hydrateFromBytes(stateLog.readRound(round))).isEqualTo(paxosValue);
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
