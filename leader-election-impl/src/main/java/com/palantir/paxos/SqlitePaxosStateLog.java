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

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Supplier;

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
                "CREATE TABLE dual (id BIGINT, CONSTRAINT "
                        + "pk_dual"
                        + " PRIMARY KEY (id))",
                "already exists"
        );

        try {
            ResultSet x = connectionSupplier.get().prepareStatement("SELECT sql FROM sqlite_master;").executeQuery();
            System.out.println(x);
        } catch (SQLException e) {
            e.printStackTrace();
        }


        executeIgnoringError(
                "INSERT INTO dual (id) SELECT 1 WHERE NOT EXISTS ( SELECT id FROM dual WHERE id = 1 )",
                "duplicate key"
        );
    }

    @Override
    public void writeRound(long seq, V round) {
        System.out.println("42");
    }

    @Override
    public byte[] readRound(long seq) {
        try {
            return BigInteger.valueOf(connectionSupplier.get().prepareStatement(
                    "SELECT 1 FROM dual;"
            ).executeQuery().getInt(0)).toByteArray();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getLeastLogEntry() {
        return 0;
    }

    @Override
    public long getGreatestLogEntry() {
        return 0;
    }

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
}
