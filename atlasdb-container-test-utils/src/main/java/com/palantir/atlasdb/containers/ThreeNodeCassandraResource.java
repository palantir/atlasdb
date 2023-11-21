/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.containers;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.atlasdb.keyvalue.impl.KvsManager;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManagerV2;
import com.palantir.atlasdb.keyvalue.impl.TransactionManagerManager;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Supplier;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class ThreeNodeCassandraResource
        implements BeforeAllCallback, AfterAllCallback, KvsManager, TransactionManagerManager {
    private final Supplier<KeyValueService> supplier;
    private TestResourceManagerV2 testResourceManager;

    public ThreeNodeCassandraResource() {
        this.supplier = () -> CassandraKeyValueServiceImpl.createForTesting(
                ThreeNodeCassandraCluster.KVS_CONFIG, ThreeNodeCassandraCluster.KVS_RUNTIME_CONFIG);
    }

    @Override
    public void beforeAll(ExtensionContext var1) throws IOException, InterruptedException {
        ContainersV2 containers = new ContainersV2(var1.getRequiredTestClass()).with(new ThreeNodeCassandraCluster());
        testResourceManager = new TestResourceManagerV2(supplier);
        containers.beforeAll(var1);
    }

    @Override
    public void afterAll(ExtensionContext var1) {
        testResourceManager.afterAll(var1);
    }

    /**
     * Returns the memoized instance of the {@link CassandraKeyValueService} given by the supplier from the constructor.
     */
    @Override
    public CassandraKeyValueService getDefaultKvs() {
        return (CassandraKeyValueService) testResourceManager.getDefaultKvs();
    }

    @Override
    public void registerKvs(KeyValueService kvs) {
        testResourceManager.registerKvs(kvs);
    }

    @Override
    public void registerTransactionManager(TransactionManager manager) {
        testResourceManager.registerTransactionManager(manager);
    }

    @Override
    public Optional<TransactionManager> getLastRegisteredTransactionManager() {
        return testResourceManager.getLastRegisteredTransactionManager();
    }
}
