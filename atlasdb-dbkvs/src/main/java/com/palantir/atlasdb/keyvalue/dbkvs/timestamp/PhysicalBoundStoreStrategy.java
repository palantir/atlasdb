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

package com.palantir.atlasdb.keyvalue.dbkvs.timestamp;

import com.palantir.nexus.db.DBType;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.OptionalLong;
import java.util.function.Function;

/**
 * A {@link PhysicalBoundStoreStrategy} is responsible for interaction between a timestamp bound store and the physical
 * storage of these values.
 */
public interface PhysicalBoundStoreStrategy {
    void createTimestampTable(Connection connection, Function<Connection, DBType> dbTypeExtractor) throws SQLException;

    OptionalLong readLimit(Connection connection) throws SQLException;

    void writeLimit(Connection connection, long limit) throws SQLException;

    void createLimit(Connection connection, long limit) throws SQLException;
}
