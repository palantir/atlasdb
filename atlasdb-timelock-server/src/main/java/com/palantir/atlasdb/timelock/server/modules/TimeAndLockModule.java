/**
 * Copyright 2016 Palantir Technologies
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

package com.palantir.atlasdb.timelock.server.modules;

import javax.inject.Singleton;

import com.google.common.base.Supplier;
import com.palantir.atlasdb.factory.ServiceDiscoveringAtlasSupplier;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.proxy.AwaitingLeadershipProxy;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.TimestampService;

import dagger.Module;
import dagger.Provides;

@Module
public class TimeAndLockModule {
    @Provides
    @Singleton
    public ServiceDiscoveringAtlasSupplier provideAtlasSupplier(KeyValueServiceConfig kvsConfig) {
        return new ServiceDiscoveringAtlasSupplier(kvsConfig);
    }

    @Provides
    @Singleton
    public TimestampService provideTimestampService(LeaderElectionService leaderElectionService, ServiceDiscoveringAtlasSupplier atlasSupplier) {
        Supplier<TimestampService> localTimestampService = atlasSupplier::getTimestampService;
        return AwaitingLeadershipProxy.newProxyInstance(TimestampService.class, localTimestampService, leaderElectionService);
    }

    @Provides
    @Singleton
    public RemoteLockService provideLockService(LeaderElectionService leaderElectionService) {
        return AwaitingLeadershipProxy.newProxyInstance(RemoteLockService.class, LockServiceImpl::create, leaderElectionService);
    }
}
