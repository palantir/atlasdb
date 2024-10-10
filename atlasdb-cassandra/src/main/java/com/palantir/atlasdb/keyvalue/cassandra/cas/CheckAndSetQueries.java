/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.cassandra.cas;

import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.cassandra.CqlQuery;
import com.palantir.atlasdb.keyvalue.cassandra.CqlUtilities;
import com.palantir.atlasdb.keyvalue.cassandra.ImmutableCqlQuery;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

final class CheckAndSetQueries {
    private CheckAndSetQueries() {
        // Static Factory
    }

    static CqlQuery getQueryForRequest(CheckAndSetRequest request) {
        return request.oldValue().map(unused -> updateIfMatching(request)).orElseGet(() -> insertIfNotExists(request));
    }

    private static CqlQuery insertIfNotExists(CheckAndSetRequest request) {
        Preconditions.checkState(
                request.oldValue().isEmpty(),
                "insertIfNotExists queries should only be made if we don't have an old value");
        return ImmutableCqlQuery.builder()
                .safeQueryFormat(
                        "INSERT INTO \"%s\" (key, column1, column2, value)" + " VALUES (%s, %s, %s, %s) IF NOT EXISTS;")
                .addArgs(
                        LoggingArgs.internalTableName(request.table()),
                        UnsafeArg.of(
                                "row",
                                CqlUtilities.encodeCassandraHexBytes(
                                        request.cell().getRowName())),
                        UnsafeArg.of(
                                "column",
                                CqlUtilities.encodeCassandraHexBytes(
                                        request.cell().getColumnName())),
                        SafeArg.of(
                                "cassandraTimestamp",
                                CqlUtilities.CASSANDRA_REPRESENTATION_OF_ATLASDB_ATOMIC_TABLE_TIMESTAMP),
                        UnsafeArg.of("newValue", CqlUtilities.encodeCassandraHexBytes(request.newValue())))
                .build();
    }

    private static CqlQuery updateIfMatching(CheckAndSetRequest request) {
        Preconditions.checkState(
                request.oldValue().isPresent(),
                "updateIfMatching queries should only be made if we do have an old value");
        byte[] data = request.newValue();
        return ImmutableCqlQuery.builder()
                .safeQueryFormat("UPDATE \"%s\" SET value=%s WHERE key=%s AND column1=%s AND column2=%s IF value=%s;")
                .addArgs(
                        LoggingArgs.internalTableName(request.table()),
                        UnsafeArg.of("newValue", CqlUtilities.encodeCassandraHexBytes(data)),
                        UnsafeArg.of(
                                "row",
                                CqlUtilities.encodeCassandraHexBytes(
                                        request.cell().getRowName())),
                        UnsafeArg.of(
                                "column",
                                CqlUtilities.encodeCassandraHexBytes(
                                        request.cell().getColumnName())),
                        SafeArg.of(
                                "cassandraTimestamp",
                                CqlUtilities.CASSANDRA_REPRESENTATION_OF_ATLASDB_ATOMIC_TABLE_TIMESTAMP),
                        UnsafeArg.of(
                                "oldValue",
                                CqlUtilities.encodeCassandraHexBytes(
                                        request.oldValue().get())))
                .build();
    }
}
