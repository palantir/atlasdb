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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;

public class WriteTransactionPerfTest extends AbstractPerfTest {

    private static final Logger log = LoggerFactory.getLogger(WriteTransactionPerfTest.class);

    private static final TableReference TABLE = TableReference.create(Namespace.create("test"), "test");

    private final TransactionManager txnManager;

    public static Map<String, Object> execute(AtlasDbConfig config, int numClients, int requestsPerClient) {
        SerializableTransactionManager txnManager = TransactionManagers.create(config, ImmutableSet.of(), res -> { }, true);
        txnManager.getKeyValueService().createTable(TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);

        try {
            return new WriteTransactionPerfTest(txnManager, numClients, requestsPerClient).execute();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
           txnManager.close();
        }
    }

    private WriteTransactionPerfTest(TransactionManager txnManager, int numClients, int requestsPerClient) {
        super(numClients, requestsPerClient);

        this.txnManager = txnManager;
    }

    @Override
    public void performOneCall() {
        txnManager.runTaskWithRetry(txn -> {
            byte[] data = UUID.randomUUID().toString().getBytes();
            txn.put(TABLE, ImmutableMap.of(Cell.create(data, data), data));
            return null;
        });
    }

}
