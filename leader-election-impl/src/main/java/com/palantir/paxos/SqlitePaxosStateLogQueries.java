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

import java.util.OptionalLong;
import java.util.Set;

import org.jdbi.v3.sqlobject.SingleValue;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindPojo;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import com.palantir.common.persist.Persistable;

public interface SqlitePaxosStateLogQueries {
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

    @SqlQuery("SELECT DISTINCT namespace, useCase FROM paxosLog")
    Set<NamespaceAndUseCase> getAllNamespaceAndUseCaseTuples();

    @SqlQuery("SELECT seq, val FROM paxosLog WHERE namespace = :namespace.value AND useCase = :useCase AND seq > :seq")
    Set<PaxosRound<PaxosValue>> getLearnerLogsSince(
            @BindPojo("namespace") Client namespace,
            @Bind("useCase") String useCase,
            @Bind("seq") long seq);

    @SqlQuery("SELECT seq, val FROM paxosLog WHERE namespace = :namespace.value AND useCase = :useCase AND seq > :seq")
    Set<PaxosRound<PaxosAcceptorState>> getAcceptorLogsSince(
            @BindPojo("namespace") Client namespace,
            @Bind("useCase") String useCase,
            @Bind("seq") long seq);
}
