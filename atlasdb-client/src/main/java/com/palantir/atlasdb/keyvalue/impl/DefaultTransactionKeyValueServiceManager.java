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

import com.palantir.atlasdb.cell.api.TransactionKeyValueService;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.TransactionKeyValueServiceManager;
import java.util.Optional;
import java.util.function.LongSupplier;

public final class DefaultTransactionKeyValueServiceManager implements TransactionKeyValueServiceManager {

    private final Optional<KeyValueService> delegate;
    private final TransactionKeyValueService transactionKeyValueService;

    public DefaultTransactionKeyValueServiceManager(KeyValueService delegate) {
        this.delegate = Optional.of(delegate);
        this.transactionKeyValueService = new DefaultTransactionKeyValueService(delegate);
    }

    @Override
    public TransactionKeyValueService getTransactionKeyValueService(LongSupplier _timestampSupplier) {
        return transactionKeyValueService;
    }

    @Override
    public Optional<KeyValueService> getKeyValueService() {
        return delegate;
    }

    @Override
    public void close() {
        // TODO(jakubk): I don't think this is entirely correct, because we shouldn't own the KeyValueService,
        // especially if it's being reused in some way. However, the cleanup codepaths in SnapshotTransactionManager
        // currently rely on this close method to close everything up.
        delegate.get().close();
    }
}
