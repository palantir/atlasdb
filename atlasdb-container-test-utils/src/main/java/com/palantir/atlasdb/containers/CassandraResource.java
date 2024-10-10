/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

import com.datastax.driver.core.Cluster;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.atlasdb.keyvalue.impl.KvsManager;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.keyvalue.impl.TransactionManagerManager;
import com.palantir.atlasdb.limiter.NoOpAtlasClientLimiter;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.refreshable.Refreshable;
import java.net.Proxy;
import java.util.Optional;
import java.util.function.Supplier;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class CassandraResource implements BeforeAllCallback, AfterAllCallback, KvsManager, TransactionManagerManager {
    private final CassandraContainer containerInstance = new CassandraContainer();
    private final Supplier<KeyValueService> supplier;
    private Containers containers;
    private TestResourceManager testResourceManager;
    private Proxy socksProxy;

    public CassandraResource() {
        this.supplier = () -> CassandraKeyValueServiceImpl.createForTesting(
                getConfig(), getRuntimeConfig(), new NoOpAtlasClientLimiter());
    }

    public CassandraResource(Supplier<KeyValueService> supplier) {
        this.supplier = supplier;
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        containers = new Containers(extensionContext.getRequiredTestClass()).with(containerInstance);
        testResourceManager = new TestResourceManager(supplier);
        containers.beforeAll(extensionContext);
        socksProxy = Containers.getSocksProxy(containerInstance.getServiceName());
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) {
        testResourceManager.afterAll(extensionContext);
    }

    /**
     * Returns the memoized instance of the {@link CassandraKeyValueService} given by the supplier from the
     * constructor.
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

    public CassandraKeyValueServiceConfig getConfig() {
        return containerInstance.getConfigWithProxy(socksProxy.address());
    }

    public Supplier<Cluster.Builder> getClusterBuilderWithProxy() {
        return containerInstance.getClusterBuilderWithProxy(socksProxy.address());
    }

    public Refreshable<CassandraKeyValueServiceRuntimeConfig> getRuntimeConfig() {
        return containerInstance.getRuntimeConfig();
    }
}
