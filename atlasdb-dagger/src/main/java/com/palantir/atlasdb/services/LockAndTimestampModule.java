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
package com.palantir.atlasdb.services;

import com.palantir.atlasdb.factory.ServiceDiscoveringAtlasSupplier;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.lock.LockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.lock.v2.TimelockService;
import com.palantir.refreshable.Refreshable;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.TimestampService;
import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import java.util.function.Supplier;
import javax.inject.Singleton;

@Module(includes = LockAndTimestampModule.Bindings.class)
public class LockAndTimestampModule {

    @Module
    public abstract static class Bindings {
        @Binds
        abstract TimestampService asTimestampService(ManagedTimestampService unused);
    }

    @Provides
    @Singleton
    public TransactionManagers.LockAndTimestampServices provideLockAndTimestampServices(
            MetricsManager metricsManager, ServicesConfig config) {
        ServiceDiscoveringAtlasSupplier atlasSupplier = config.atlasDbSupplier(metricsManager);
        Supplier<ManagedTimestampService> managedTimestampService = atlasSupplier::getManagedTimestampService;

        return TransactionManagers.createLockAndTimestampServicesForCli(
                metricsManager,
                config.atlasDbConfig(),
                Refreshable.only(config.atlasDbRuntimeConfig()),
                resource -> { },
                LockServiceImpl::create,
                managedTimestampService,
                config.atlasDbSupplier(metricsManager).getTimestampStoreInvalidator(),
                "cli");
    }

    @Provides
    @Singleton
    public TimelockService provideTimelockService(TransactionManagers.LockAndTimestampServices lts) {
        return lts.timelock();
    }

    @Provides
    @Singleton
    public ManagedTimestampService provideManagedTimestampService(TransactionManagers.LockAndTimestampServices lts) {
        return lts.managedTimestampService();
    }

    @Provides
    @Singleton
    public LockService provideLockService(TransactionManagers.LockAndTimestampServices lts) {
        return lts.lock();
    }

}
