/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.services;

import javax.inject.Singleton;

import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.lock.LockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.TimestampService;

import dagger.Module;
import dagger.Provides;

@Module
public class LockAndTimestampModule {

    @Provides
    @Singleton
    public TransactionManagers.LockAndTimestampServices provideLockAndTimestampServices(ServicesConfig config) {
        return TransactionManagers.createLockAndTimestampServicesForCli(
                config.atlasDbConfig(),
                config::atlasDbRuntimeConfig,
                resource -> { },
                LockServiceImpl::create,
                () -> config.atlasDbSupplier().getTimestampService(),
                config.atlasDbSupplier().getTimestampStoreInvalidator(),
                "cli");
    }

    @Provides
    @Singleton
    public TimelockService provideTimelockService(TransactionManagers.LockAndTimestampServices lts) {
        return lts.timelock();
    }

    @Provides
    @Singleton
    public TimestampService provideTimestampService(TransactionManagers.LockAndTimestampServices lts) {
        return lts.timestamp();
    }

    @Provides
    @Singleton
    public LockService provideLockService(TransactionManagers.LockAndTimestampServices lts) {
        return lts.lock();
    }

}
