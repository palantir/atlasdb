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

package com.palantir.atlasdb.keyvalue.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.junit.rules.ExternalResource;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.TransactionManager;

public class CloseableResourceManager extends ExternalResource {
    private final Supplier<KeyValueService> createKvsSupplier;
    private final Supplier<KeyValueService> getKvsSupplier;
    private final List<AutoCloseable> closeableResources = new ArrayList<>();

    private KeyValueService keyValueService = null;
    private TransactionManager transactionManager = null;

    public CloseableResourceManager(Supplier<KeyValueService> kvsSupplier) {
        this.createKvsSupplier = () -> {
            KeyValueService kvs = kvsSupplier.get();
            registerCloseable(kvs);
            return kvs;
        };
        this.getKvsSupplier = Suppliers.memoize(createKvsSupplier::get);
    }

    public KeyValueService getKvs() {
        return getKvsSupplier.get();
    }

    public KeyValueService createKvs() {
        return createKvsSupplier.get();
    }

    public void registerKvs(KeyValueService kvs) {
        keyValueService = kvs;
        registerCloseable(kvs);
    }

    public Optional<KeyValueService> getRegisteredKvs() {
        return Optional.ofNullable(keyValueService);
    }

    public void registerTransactionManager(TransactionManager manager) {
        transactionManager = manager;
        registerCloseable(manager);
    }

    public Optional<TransactionManager> getRegisteredTransactionManager() {
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
