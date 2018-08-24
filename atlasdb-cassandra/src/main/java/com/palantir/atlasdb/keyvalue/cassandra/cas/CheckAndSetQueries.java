/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.keyvalue.cassandra.cas;

import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.cassandra.CqlQuery;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

final class CheckAndSetQueries {
    private static final long CASSANDRA_TIMESTAMP = -1L;
    private static final String CASSANDRA_PREFIX = "0x";

    private CheckAndSetQueries() {
        // Static Factory
    }

    static CqlQuery getQueryForRequest(CheckAndSetRequest request) {
        return request.oldValue().map(unused -> updateIfMatching(request)).orElseGet(() -> insertIfNotExists(request));
    }

    private static CqlQuery insertIfNotExists(CheckAndSetRequest request) {
        Preconditions.checkState(!request.oldValue().isPresent(),
                "insertIfNotExists queries should only be made if we don't have an old value");
        return new CqlQuery(
                "INSERT INTO \"%s\" (key, column1, column2, value) VALUES (%s, %s, %s, %s) IF NOT EXISTS;",
                LoggingArgs.internalTableName(request.table()),
                UnsafeArg.of("row", encodeCassandraHexString(request.cell().getRowName())),
                UnsafeArg.of("column", encodeCassandraHexString(request.cell().getColumnName())),
                SafeArg.of("cassandraTimestamp", CASSANDRA_TIMESTAMP),
                UnsafeArg.of("newValue", encodeCassandraHexString(request.newValue())));
    }

    private static CqlQuery updateIfMatching(CheckAndSetRequest request) {
        Preconditions.checkState(request.oldValue().isPresent(),
                "updateIfMatching queries should only be made if we do have an old value");
        return new CqlQuery(
                "UPDATE \"%s\" SET value=%s WHERE key=%s AND column1=%s AND column2=%s IF value=%s;",
                LoggingArgs.internalTableName(request.table()),
                UnsafeArg.of("newValue", encodeCassandraHexString(request.newValue())),
                UnsafeArg.of("row", encodeCassandraHexString(request.cell().getRowName())),
                UnsafeArg.of("column", encodeCassandraHexString(request.cell().getColumnName())),
                SafeArg.of("cassandraTimestamp", CASSANDRA_TIMESTAMP),
                UnsafeArg.of("oldValue", encodeCassandraHexString(request.oldValue().get())));
    }

    private static String encodeCassandraHexString(byte[] data) {
        return CASSANDRA_PREFIX + BaseEncoding.base16().upperCase().encode(data);
    }
}
