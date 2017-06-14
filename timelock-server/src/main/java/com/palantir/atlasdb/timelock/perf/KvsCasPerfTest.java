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
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;

/**
 * Created by nziebart on 5/27/17.
 */
public class KvsCasPerfTest extends AbstractPerfTest {

    private static final TableReference TABLE = TableReference.create(Namespace.create("test"), "test");

    private final KeyValueService keyValueService;

    public static Map<String, Object> execute(AtlasDbConfig config, int numClients, int requestsPerClient) {
        SerializableTransactionManager txnManager = TransactionManagers.create(config, ImmutableSet.of(), res -> { }, true);
        txnManager.getKeyValueService().createTable(TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);

        try {
            return new KvsCasPerfTest(txnManager.getKeyValueService(), numClients, requestsPerClient).execute();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            txnManager.getKeyValueService().close();
            txnManager.close();
        }
    }

    private KvsCasPerfTest(KeyValueService keyValueService, int numClients, int numRequestsPerClient) {
        super(numClients, numRequestsPerClient);
        this.keyValueService = keyValueService;
    }

    @Override
    protected void performOneCall() {
        byte[] data = UUID.randomUUID().toString().getBytes();
        keyValueService.putUnlessExists(TABLE, ImmutableMap.of(Cell.create(data, data), data));
    }
}
