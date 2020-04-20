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
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Supplier;

import com.google.common.io.ByteStreams;
import com.palantir.common.persist.Persistable;
import com.palantir.exception.PalantirSqlException;

public class SqlitePaxosStateLog<V extends Persistable & Versionable> implements PaxosStateLog<V> {
    private final Supplier<Connection> connectionSupplier;

    public SqlitePaxosStateLog(Supplier<Connection> connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
        setup();
    }

    private void setup() {

        executeIgnoringError(
                "CREATE TABLE test (id BIGINT, val BLOB, CONSTRAINT pk_dual PRIMARY KEY (id))",
                "already exists"
        );
    }

    @Override
    public void writeRound(long seq, V round) {
        try {
            PreparedStatement preparedStatement = connectionSupplier.get().prepareStatement(
                    "INSERT INTO test (id, val) VALUES (?, ?)");
            preparedStatement.setLong(1, seq);
            preparedStatement.setBytes(2, round.persistToBytes());
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] readRound(long seq) {
        try {
            ResultSet resultSet = executeStatement(String.format("SELECT val FROM test WHERE id = %s;", seq));
            return ByteStreams.toByteArray(resultSet.getBinaryStream(1));
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getLeastLogEntry() {
        try {
            ResultSet resultSet = executeStatement("SELECT MIN(id) FROM test");
            return resultSet.getLong(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getGreatestLogEntry() {
        try {
            ResultSet resultSet = executeStatement("SELECT MAX(id) FROM test");
            return resultSet.getLong(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }    }

    @Override
    public void truncate(long toDeleteInclusive) {
        // TODO (jkong): Actually delete stuff
        throw new UnsupportedOperationException("no");
    }

    private void executeIgnoringError(String sql, String errorToIgnore) {
        try {
            connectionSupplier.get().prepareStatement(sql).execute();
        } catch (PalantirSqlException | SQLException e) {
            if (!e.getMessage().contains(errorToIgnore)) {
                throw new RuntimeException(e);
            }
        }
    }

    private ResultSet executeStatement(String statement) {
        try {
            return connectionSupplier.get().prepareStatement(statement).executeQuery();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
