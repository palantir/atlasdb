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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.function.Supplier;

import com.google.common.io.ByteStreams;
import com.palantir.common.base.Throwables;
import com.palantir.common.persist.Persistable;

public class SqlitePaxosStateLog<V extends Persistable & Versionable> implements PaxosStateLog<V> {
    private final Supplier<Connection> connectionSupplier;

    private SqlitePaxosStateLog(Supplier<Connection> connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
    }

    public static <V extends Persistable & Versionable> PaxosStateLog<V> createInitialized(Supplier<Connection> conn) {
        SqlitePaxosStateLog<V> log = new SqlitePaxosStateLog<>(conn);
        log.initialize();
        return log;
    }

    private void initialize() {
        executeVoid("CREATE TABLE IF NOT EXISTS paxosLog (seq BIGINT, val BLOB, CONSTRAINT pk_dual PRIMARY KEY (seq))");
    }

    @Override
    public void writeRound(long seq, V round) {
        try {
            PreparedStatement preparedStatement = connectionSupplier.get().prepareStatement(
                    "INSERT OR REPLACE INTO paxosLog (seq, val) VALUES (?, ?)");
            preparedStatement.setLong(1, seq);
            preparedStatement.setBytes(2, round.persistToBytes());
            preparedStatement.execute();
        } catch (SQLException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    @Override
    public byte[] readRound(long seq) {
        return executeStatement(String.format("SELECT val FROM paxosLog WHERE seq = %s", seq))
                .map(SqlitePaxosStateLog::getByteArrayUnchecked)
                .orElse(null);
    }

    @Override
    public long getLeastLogEntry() {
        return executeStatement("SELECT MIN(seq) FROM paxosLog")
                .map(SqlitePaxosStateLog::getLongResultUnchecked)
                .orElse(PaxosAcceptor.NO_LOG_ENTRY);
    }

    @Override
    public long getGreatestLogEntry() {
        return executeStatement("SELECT MAX(seq) FROM paxosLog")
                .map(SqlitePaxosStateLog::getLongResultUnchecked)
                .orElse(PaxosAcceptor.NO_LOG_ENTRY);
    }

    @Override
    public void truncate(long toDeleteInclusive) {
        executeVoid(String.format("DELETE FROM paxosLog WHERE seq <= %s", toDeleteInclusive));
    }

    private void executeVoid(String statement) {
        try {
            connectionSupplier.get().prepareStatement(statement).execute();
        } catch (SQLException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    private Optional<ResultSet> executeStatement(String statement) {
        try {
            ResultSet resultSet = connectionSupplier.get().prepareStatement(statement).executeQuery();
            return resultSet.isClosed() ? Optional.empty() : Optional.of(resultSet);
        } catch (SQLException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    private static long getLongResultUnchecked(ResultSet resultSet) {
        try {
            long candidate = resultSet.getLong(1);
            return resultSet.wasNull() ? PaxosAcceptor.NO_LOG_ENTRY : candidate;
        } catch (SQLException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    private static byte[] getByteArrayUnchecked(ResultSet resultSet) {
        try {
            return ByteStreams.toByteArray(resultSet.getBinaryStream(1));
        } catch (SQLException | IOException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }
}
