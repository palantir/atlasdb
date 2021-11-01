/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.memory;

import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.logsafe.Preconditions;
import com.palantir.timestamp.AutoDelegate_TimestampService;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.TimestampService;

public final class AsyncInitializeableInMemoryTimestampService extends AsyncInitializer
        implements AutoDelegate_TimestampService, ManagedTimestampService {
    private final KeyValueService kvs;
    private final InMemoryTimestampService timestampService = new InMemoryTimestampService();

    private AsyncInitializeableInMemoryTimestampService(KeyValueService kvs) {
        this.kvs = kvs;
    }

    /**
     * Creates an {@link InMemoryTimestampService} that is asynchronously initialized and ready to use once the kvs is
     * ready. This should only be useful for tests.
     *
     * @param kvs KeyValueService that must be ready before the returned TimestampService can be initialized.
     * @return the asynchronously initialized PersistentTimestampService
     */
    public static AsyncInitializeableInMemoryTimestampService initializeWhenKvsIsReady(KeyValueService kvs) {
        AsyncInitializeableInMemoryTimestampService service = new AsyncInitializeableInMemoryTimestampService(kvs);
        service.initialize(true);
        return service;
    }

    @Override
    public TimestampService delegate() {
        checkInitialized();
        return timestampService;
    }

    @Override
    protected void tryInitialize() {
        Preconditions.checkState(kvs.isInitialized());
    }

    @Override
    protected String getInitializingClassName() {
        return "AsyncInitializeableInMemoryTimestampService";
    }

    @Override
    protected int sleepIntervalInMillis() {
        return 1_000;
    }

    @Override
    public void fastForwardTimestamp(long _currentTimestamp) {
        throw new UnsupportedOperationException("Not implemented in test class");
    }

    @Override
    public String ping() {
        throw new UnsupportedOperationException("Not implemented in test class");
    }
}
