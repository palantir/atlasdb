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

package com.palantir.atlasdb.containers;

import java.util.Optional;
import java.util.function.Supplier;

import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.atlasdb.keyvalue.impl.KvsManager;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.keyvalue.impl.TmManager;
import com.palantir.atlasdb.transaction.api.TransactionManager;

public class CassandraResource extends ExternalResource implements KvsManager, TmManager {
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static final Optional<LeaderConfig> LEADER_CONFIG = CassandraContainer.LEADER_CONFIG;
    private final CassandraContainer containerInstance = new CassandraContainer();
    private final Supplier<KeyValueService> supplier;
    private Containers containers;
    private TestResourceManager testResourceManager;

    public CassandraResource() {
        this.supplier = () -> CassandraKeyValueServiceImpl.createForTesting(
                containerInstance.getConfig(), CassandraContainer.LEADER_CONFIG);
    }

    public CassandraResource(Supplier<KeyValueService> supplier) {
        this.supplier = supplier;
    }

    public Statement apply(Statement base, Description description) {
        containers = new Containers(description.getTestClass()).with(containerInstance);
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
     * */
    @Override
    public CassandraKeyValueService getDefaultKvs() {
        return (CassandraKeyValueService) testResourceManager.getDefaultKvs();
    }

    @Override
    public void registerKvs(KeyValueService kvs) {
        testResourceManager.registerKvs(kvs);
    }

    @Override
    public Optional<KeyValueService> getLastRegisteredKvs() {
        return testResourceManager.getLastRegisteredKvs();
    }

    @Override
    public void registerTm(TransactionManager manager) {
        testResourceManager.registerTm(manager);
    }

    @Override
    public Optional<TransactionManager> getLastRegisteredTm() {
        return testResourceManager.getLastRegisteredTm();
    }

    public CassandraKeyValueServiceConfig getConfig() {
        return containerInstance.getConfig();
    }
}
