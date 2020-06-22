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
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.keyvalue.impl.TransactionManagerManager;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import java.util.Optional;
import java.util.function.Supplier;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class ThreeNodeCassandraResource extends ExternalResource implements KvsManager, TransactionManagerManager {
    private final Supplier<KeyValueService> supplier;
    private Containers containers;
    private TestResourceManager testResourceManager;

    public ThreeNodeCassandraResource() {
        this.supplier = () ->
                CassandraKeyValueServiceImpl.createForTesting(ThreeNodeCassandraCluster.KVS_CONFIG);
    }

    @Override public Statement apply(Statement base, Description description) {
        containers = new Containers(description.getTestClass()).with(new ThreeNodeCassandraCluster());
        testResourceManager = new TestResourceManager(supplier);
        return super.apply(base, description);
    }

    @Override
    public void before() throws Throwable {
        containers.before();
    }

    @Override
    public void after() {
        testResourceManager.after();
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
