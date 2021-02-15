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

package com.palantir.atlasdb.keyvalue.impl;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.junit.rules.ExternalResource;

public class TestResourceManager extends ExternalResource implements KvsManager, TransactionManagerManager {
    private final Supplier<KeyValueService> getKvsSupplier;
    private final List<AutoCloseable> closeableResources = new ArrayList<>();

    private TransactionManager transactionManager = null;

    public TestResourceManager(Supplier<KeyValueService> kvsSupplier) {
        this.getKvsSupplier = Suppliers.memoize(() -> {
            KeyValueService kvs = kvsSupplier.get();
            registerCloseable(kvs);
            return kvs;
        });
    }

    public static TestResourceManager inMemory() {
        return new TestResourceManager(() -> new InMemoryKeyValueService(false));
    }

    @Override
    public KeyValueService getDefaultKvs() {
        return getKvsSupplier.get();
    }

    @Override
    public void registerKvs(KeyValueService kvs) {
        registerCloseable(kvs);
    }

    @Override
    public void registerTransactionManager(TransactionManager manager) {
        transactionManager = manager;
        registerCloseable(manager);
    }

    @Override
    public Optional<TransactionManager> getLastRegisteredTransactionManager() {
        return Optional.ofNullable(transactionManager);
    }

    private void registerCloseable(AutoCloseable resource) {
        closeableResources.add(resource);
    }

    @Override
    public void after() {
        closeableResources.forEach(resource -> {
            try {
                resource.close();
            } catch (Exception e) {
                // only best effort for tests
            }
        });
    }
}
