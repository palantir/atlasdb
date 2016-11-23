/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.cassandra;

import javax.ws.rs.QueryParam;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.timestamp.TimestampAdminService;

public class CassandraTimestampAdminService implements TimestampAdminService {
    private static final Logger log = LoggerFactory.getLogger(CassandraTimestampAdminService.class);

    private final CassandraClientPool clientPool;

    public CassandraTimestampAdminService(KeyValueService rawKvs) {
        Preconditions.checkArgument(rawKvs instanceof CassandraKeyValueService,
                String.format("CassandraTimestampAdminServices must be created with a CassandraKeyValueService, "
                        + "but %s was provided", rawKvs.getClass()));

        clientPool = ((CassandraKeyValueService) rawKvs).clientPool;
    }

    @Override
    public long getUpperBoundTimestamp() {
        return clientPool.runWithRetry(client -> {
            ColumnOrSuperColumn result = CassandraTimestampUtils.readCassandraTimestamp(client);

            if (result == null) {
                return 0L;
            }
            return PtBytes.toLong(result.getColumn().getValue());
        });
    }

    @Override
    public void fastForwardTimestamp(@QueryParam("newMinimum") long newMinimumTimestamp) {
    }

    @Override
    public void invalidateTimestamps() {
    }
}
