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
     * one table, additional columns 50M entries
     * 100 min PT0.145S
     * 100 max PT0.122S
     * 10K reads PT9.304S
     *
     * 100 min PT0.152S
     * 100 max PT0.108S
     * 10K reads PT9.003S
     * DB size 3_918_503_936
     *
     * 100 min PT0.141S
     * 100 max PT0.121S
     * 10K reads PT9.422S
     * DB size 3_918_503_936
     *
     * 100 min PT0.13S
     * 100 max PT0.087S
     * 10K reads PT9.95S
     * 10K reads PT8.887S
     * 10K reads PT9.165S
     * 10K reads PT9.248S
     * 10K reads PT10.314S
     * 10K new writes PT17.186S
     * 10K new writes PT18.015S
     * 10K new writes PT18.625S
     * 10K new writes PT18.316S
     * 10K new writes PT18.546S
     * 10K overwrites PT19.952S
     * 10K overwrites PT19.485S
     * 10K overwrites PT19.658S
     * 10K overwrites PT19.858S
     * 10K overwrites PT18.488S
     *
     * 5 tables, each 10M entries
     * 100 min PT0.148S
     * 100 max PT0.126S
     * 10K reads PT9.417S
     *
     * 100 min PT0.143S
     * 100 max PT0.127S
     * 10K reads PT9.836S
     * DB size 3_113_611_264
     *
     * 100 min PT0.179S
     * 100 max PT0.135S
     * 10K reads PT8.621S
     * 20K writes PT53.3S
     * DB size 3_114_942_464
     *
     * 100 min PT0.129S
     * 100 max PT0.092S
     * 10K reads PT10.362S
     * 10K reads PT9.395S
     * 10K reads PT10.597S
     * 10K reads PT12.24S
     * 10K reads PT11.331S
     * 10K new writes PT17.302S
     * 10K new writes PT18.475S
     * 10K new writes PT17.473S
     * 10K new writes PT17.221S
     * 10K new writes PT16.158S
     * 10K overwrites PT18.02S
     * 10K overwrites PT19.319S
     * 10K overwrites PT20.462S
     * 10K overwrites PT18.764S
     * 10K overwrites PT18.371S
     *
     * @throws IOException
     */
    @Test
    public void test2() throws IOException {
        List<PaxosStateLog<PaxosValue>> logs = Stream.of("gera", "ghretwh", "erthw", "btwrhb", "bnrtwhnw")
                .map(x -> SqlitePaxosStateLog.<PaxosValue>create(x, connectionSupplier))
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
        for (int i = 0; i < 100; i++) {
            logs.get(i % 5).getLeastLogEntry();
        }
        System.out.println("     * 100 min " + Duration.between(startTime, Instant.now()));
        startTime = Instant.now();
        for (int i = 0; i < 100; i++) {
            logs.get(i % 5).getGreatestLogEntry();
        }
        System.out.println("     * 100 max " + Duration.between(startTime, Instant.now()));
        for (int j = 0; j < 5; j++) {
            startTime = Instant.now();
            for (long i = 1_000_000 + j; i < 10_000_000; i += 800) {
                logs.get((int) i % 5).readRound(i);
            }
            System.out.println("     * 10K reads " + Duration.between(startTime, Instant.now()));
        }
        for (int j = 0; j < 5; j++) {
            List<PaxosRound<PaxosValue>> finalRounds = KeyedStream.of(
                    LongStream.rangeClosed(10_000_000 + 10_000 * j, 10_010_000 + 10_000 * j).boxed())
                    .map(SqliteBenchmarkThrowaway::valueForRound)
                    .map((a, b) -> ImmutablePaxosRound.<PaxosValue>builder().sequence(a).value(b).build()).values()
                    .collect(Collectors.toList());
            startTime = Instant.now();
            finalRounds.forEach(
                    round -> logs.get((int) round.sequence() % 5).writeRound(round.sequence(), round.value()));
            System.out.println("     * 10K new writes " + Duration.between(startTime, Instant.now()));
        }
        for (int j = 0; j < 5; j++) {
            List<PaxosRound<PaxosValue>> finalRounds = KeyedStream.of(
                    LongStream.rangeClosed(1_000_000 + 10_000 * j, 1_010_000 + 10_000 * j).boxed())
                    .map(SqliteBenchmarkThrowaway::valueForRound)
                    .map((a, b) -> ImmutablePaxosRound.<PaxosValue>builder().sequence(a).value(b).build()).values()
                    .collect(Collectors.toList());
            startTime = Instant.now();
            finalRounds.forEach(
                    round -> logs.get((int) round.sequence() % 5).writeRound(round.sequence(), round.value()));
            System.out.println("     * 10K overwrites " + Duration.between(startTime, Instant.now()));
        }
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
