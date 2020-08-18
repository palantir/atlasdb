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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.timelock.management.PersistentNamespaceLoader;
import com.palantir.atlasdb.timelock.management.SqliteNamespaceLoader;
import com.zaxxer.hikari.HikariDataSource;

public class CorruptionStateVerifier {
    // This looks funny but is correct for the type to mismatch.
    private final SqlitePaxosStateLog<PaxosAcceptorState> one;
    private final SqlitePaxosStateLog<PaxosAcceptorState> two;
    private final SqlitePaxosStateLog<PaxosAcceptorState> three;
    public CorruptionStateVerifier(
            SqlitePaxosStateLog<PaxosAcceptorState> one,
            SqlitePaxosStateLog<PaxosAcceptorState> two,
            SqlitePaxosStateLog<PaxosAcceptorState> three) {
        this.one = one;
        this.two = two;
        this.three = three;
    }
    public boolean agreeAtSequence(long seq) {
        byte[] a = one.readRound(seq);
        byte[] b = two.readRound(seq);
        byte[] c = three.readRound(seq);
        Set<byte[]> set = Sets.newTreeSet(UnsignedBytes.lexicographicalComparator());
        if (a != null) {
            set.add(a);
        }
        if (b != null) {
            set.add(b);
        }
        if (c != null) {
            set.add(c);
        }
        if (set.size() > 1) {
            System.out.println("POSSIBLE CORRUPTION: round " + seq + ", values " + set);
            return false;
        }
        System.out.println("CORRECT:" + seq);
        return true;
    }
    // returns discrepancies
    public List<Long> goThroughLogs(long start, long finish) {
        return LongStream.rangeClosed(start, finish)
                .filter(x -> !agreeAtSequence(x))
                .boxed()
                .collect(Collectors.toList());
    }
    // print
    public static void main(String[] args) {
        Path path1 = Paths.get("sqliteData-timelock-4.db");
        Path path2 = Paths.get("sqliteData-timelock-5.db");
        Path path3 = Paths.get("sqliteData-timelock-6.db");
        HikariDataSource db1 = SqliteConnections.getPooledDataSource2(path1);
        HikariDataSource db2 = SqliteConnections.getPooledDataSource2(path2);
        HikariDataSource db3 = SqliteConnections.getPooledDataSource2(path3);
        PersistentNamespaceLoader pnl1 = SqliteNamespaceLoader.create(db1);
        PersistentNamespaceLoader pnl2 = SqliteNamespaceLoader.create(db2);
        PersistentNamespaceLoader pnl3 = SqliteNamespaceLoader.create(db3);
        Set<Client> allClients = Sets.union(pnl1.getAllPersistedNamespaces(),
                Sets.union(pnl2.getAllPersistedNamespaces(), pnl3.getAllPersistedNamespaces()));
        checkLearnersAgree(db1, db2, db3, allClients);
    }
    public static void checkLearnersAgree(HikariDataSource db1, HikariDataSource db2, HikariDataSource db3,
            Set<Client> allClients) {
        Set<Client> clientsWithDisagreements = Sets.newHashSet();
        for (Client client : allClients) {
            SqlitePaxosStateLog<PaxosAcceptorState> pas1
                    = SqlitePaxosStateLog.create(ImmutableNamespaceAndUseCase.of(client, "clientPaxos!learner"), db1);
            SqlitePaxosStateLog<PaxosAcceptorState> pas2 =
                    SqlitePaxosStateLog.create(ImmutableNamespaceAndUseCase.of(client, "clientPaxos!learner"), db2);
            SqlitePaxosStateLog<PaxosAcceptorState> pas3 =
                    SqlitePaxosStateLog.create(ImmutableNamespaceAndUseCase.of(client, "clientPaxos!learner"), db3);
            long min = Stream.of(pas1, pas2, pas3).map(PaxosStateLog::getLeastLogEntry).min(Comparator.naturalOrder())
                    .get();
            long max = Stream.of(pas1, pas2, pas3).map(PaxosStateLog::getGreatestLogEntry).max(Comparator.naturalOrder())
                    .get();
            CorruptionStateVerifier verifier = new CorruptionStateVerifier(pas1, pas2, pas3);
            List<Long> disagreements = verifier.goThroughLogs(min, max);
            System.out.println(disagreements);
            if (!disagreements.isEmpty()) {
                clientsWithDisagreements.add(client);
            }
        }
        System.out.println("LEARNERS VERIFIED: the following clients had disagreements: " + clientsWithDisagreements);
    }
    public static void checkAcceptorsAgree(HikariDataSource db1, HikariDataSource db2, HikariDataSource db3,
            Set<Client> allClients) {
        for (Client client : allClients) {
            SqlitePaxosStateLog<PaxosAcceptorState> pas1
                    = SqlitePaxosStateLog.create(ImmutableNamespaceAndUseCase.of(client, "clientPaxos!acceptor"), db1);
            SqlitePaxosStateLog<PaxosAcceptorState> pas2 =
                    SqlitePaxosStateLog.create(ImmutableNamespaceAndUseCase.of(client, "clientPaxos!acceptor"), db2);
            SqlitePaxosStateLog<PaxosAcceptorState> pas3 =
                    SqlitePaxosStateLog.create(ImmutableNamespaceAndUseCase.of(client, "clientPaxos!acceptor"), db3);
            long min = Stream.of(pas1, pas2, pas3).map(PaxosStateLog::getLeastLogEntry).min(Comparator.naturalOrder())
                    .get();
            long max = Stream.of(pas1, pas2, pas3).map(PaxosStateLog::getGreatestLogEntry).max(Comparator.naturalOrder())
                    .get();
            CorruptionStateVerifier verifier = new CorruptionStateVerifier(pas1, pas2, pas3);
            List<Long> disagreements = verifier.goThroughLogs(min, max);
            System.out.println(disagreements);
        }
    }
}