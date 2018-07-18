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
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;

class CheckAndSetQueries {
    private static final long CASSANDRA_TIMESTAMP = -1L;
    private static final String CASSANDRA_PREFIX = "0x";

    private CheckAndSetQueries() {
        // Static Factory
    }

    public static CqlQuery getQueryForRequest(CheckAndSetRequest request) {
        return request.oldValue().map(unused -> updateIfMatching(request)).orElseGet(() -> insertIfNotExists(request));
    }

    private static CqlQuery insertIfNotExists(CheckAndSetRequest request) {
        Preconditions.checkState(!request.oldValue().isPresent(),
                "insertIfNotExists queries should only be made if we don't have an old value");
        return new CqlQuery(String.format(
                "INSERT INTO \"%s\" (key, column1, column2, value) VALUES (%s, %s, %s, %s) IF NOT EXISTS;",
                AbstractKeyValueService.internalTableName(request.table()),
                encodeCassandraHexString(request.cell().getRowName()),
                encodeCassandraHexString(request.cell().getColumnName()),
                CASSANDRA_TIMESTAMP,
                encodeCassandraHexString(request.newValue())));
    }

    private static CqlQuery updateIfMatching(CheckAndSetRequest request) {
        Preconditions.checkState(request.oldValue().isPresent(),
                "updateIfMatching queries should only be made if we do have an old value");
        return new CqlQuery(String.format(
                "UPDATE \"%s\" SET value=%s WHERE key=%s AND column1=%s AND column2=%s IF value=%s;",
                AbstractKeyValueService.internalTableName(request.table()),
                encodeCassandraHexString(request.newValue()),
                encodeCassandraHexString(request.cell().getRowName()),
                encodeCassandraHexString(request.cell().getColumnName()),
                CASSANDRA_TIMESTAMP,
                encodeCassandraHexString(request.oldValue().get())));
    }

    private static String encodeCassandraHexString(byte[] data) {
        return CASSANDRA_PREFIX + BaseEncoding.base16().upperCase().encode(data);
    }
}
