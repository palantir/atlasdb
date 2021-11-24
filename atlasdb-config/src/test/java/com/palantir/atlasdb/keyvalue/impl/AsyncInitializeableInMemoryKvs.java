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

import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.keyvalue.api.AutoDelegate_KeyValueService;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetCompatibility;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.logsafe.Preconditions;
import java.time.Duration;

public final class AsyncInitializeableInMemoryKvs extends AsyncInitializer implements AutoDelegate_KeyValueService {
    private final InMemoryKeyValueService delegate;
    volatile boolean initializationShouldSucceed;

    private AsyncInitializeableInMemoryKvs(InMemoryKeyValueService delegate, boolean initializationShouldSucceed) {
        this.delegate = delegate;
        this.initializationShouldSucceed = initializationShouldSucceed;
    }

    public static KeyValueService createAndStartInit(boolean initializeAsync) {
        InMemoryKeyValueService kvs = new InMemoryKeyValueService(false);
        AsyncInitializeableInMemoryKvs wrapper = new AsyncInitializeableInMemoryKvs(kvs, !initializeAsync);
        wrapper.initialize(initializeAsync);
        return wrapper.isInitialized() ? wrapper.delegate() : wrapper;
    }

    @Override
    public KeyValueService delegate() {
        checkInitialized();
        return delegate;
    }

    @Override
    protected void tryInitialize() {
        Preconditions.checkState(initializationShouldSucceed);
    }

    @Override
    protected void cleanUpOnInitFailure() {
        initializationShouldSucceed = true;
    }

    @Override
    protected String getInitializingClassName() {
        return "AsyncInitializeableInMemoryKvs";
    }

    @Override
    public boolean supportsCheckAndSet() {
        return true;
    }

    @Override
    public CheckAndSetCompatibility getCheckAndSetCompatibility() {
        return CheckAndSetCompatibility.supportedBuilder()
                .consistentOnFailure(true)
                .supportsDetailOnFailure(true)
                .build();
    }

    @Override
    protected Duration sleepInterval() {
        return Duration.ofSeconds(1);
    }

    @Override
    public boolean shouldTriggerCompactions() {
        return false;
    }
}
