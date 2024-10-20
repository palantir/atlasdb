/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.atlasdb.cell.api.DataKeyValueService;
import com.palantir.atlasdb.cell.api.DataKeyValueServiceManager;
import com.palantir.atlasdb.cell.api.DdlManager;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import java.util.Optional;
import java.util.function.LongSupplier;

public final class DelegatingDataKeyValueServiceManager implements DataKeyValueServiceManager {

    private final Optional<KeyValueService> delegate;
    private final DataKeyValueService dataKeyValueService;
    private final DdlManager ddlManager;

    public DelegatingDataKeyValueServiceManager(KeyValueService delegate) {
        this.delegate = Optional.of(delegate);
        this.dataKeyValueService = new DelegatingDataKeyValueService(delegate);
        this.ddlManager = new DelegatingDdlManager(delegate);
    }

    @Override
    public DataKeyValueService getDataKeyValueService(LongSupplier _timestampSupplier) {
        return dataKeyValueService;
    }

    @Override
    public Optional<KeyValueService> getKeyValueService() {
        return delegate;
    }

    @Override
    public DdlManager getDdlManager() {
        return ddlManager;
    }

    @Override
    public boolean isInitialized() {
        // TODO(jakubk): This is already covered by code in TransactionManagers,
        // but again it does not hurt to be explicit for now.
        return delegate.get().isInitialized();
    }

    @Override
    public void close() {
        // TODO(jakubk): I don't think this is entirely correct, because we shouldn't own the KeyValueService,
        // especially if it's being reused in some way. However, the cleanup codepaths in SnapshotTransactionManager
        // currently rely on this close method to close everything up.
        delegate.get().close();
    }
}
