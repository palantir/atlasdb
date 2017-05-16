/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.perf;

import java.util.Map;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.timestamp.TimestampService;

public class TimestampPerfTest extends AbstractPerfTest {

    private static final TableReference TABLE = TableReference.create(Namespace.create("test"), "test");
    private final TimestampService timestampService;

    public static Map<String, Object> execute(AtlasDbConfig config, int numClients, int requestsPerClient) {
        SerializableTransactionManager txnManager = TransactionManagers.create(config, ImmutableSet.of(), res -> { }, true);
        txnManager.getKeyValueService().createTable(TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);

        try {
            return new TimestampPerfTest(txnManager.getTimestampService(), numClients, requestsPerClient).execute();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            txnManager.getKeyValueService().close();
            txnManager.close();
        }
    }

    private TimestampPerfTest(TimestampService timestampService, int numClients, int numRequestsPerClient) {
        super(numClients, numRequestsPerClient);
        this.timestampService = timestampService;
    }

    @Override
    protected void performOneCall() {
        long timestamp = timestampService.getFreshTimestamp();
        assert timestamp > 0;
    }
}
