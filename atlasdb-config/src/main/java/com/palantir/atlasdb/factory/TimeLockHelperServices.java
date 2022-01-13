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

package com.palantir.atlasdb.factory;

import com.palantir.atlasdb.keyvalue.api.LockWatchCachingConfig;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerImpl;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerInternal;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.lock.client.LockWatchStarter;
import com.palantir.lock.client.RequestBatchersFactory;
import com.palantir.lock.watch.LockWatchCache;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.immutables.value.Value;

@Value.Immutable
public interface TimeLockHelperServices {
    LockWatchManagerInternal lockWatchManager();

    RequestBatchersFactory requestBatchersFactory();

    static TimeLockHelperServices create(
            String namespace,
            MetricsManager metricsManager,
            Set<Schema> schemas,
            LockWatchStarter lockWatchStarter,
            LockWatchCachingConfig lockWatchCachingConfig,
            Supplier<Optional<RequestBatchersFactory.MultiClientRequestBatchers>> requestBatcherProvider) {

        LockWatchManagerInternal lockWatchManager =
                LockWatchManagerImpl.create(metricsManager, schemas, lockWatchStarter, lockWatchCachingConfig);
        LockWatchCache lockWatchCache = lockWatchManager.getCache();

        RequestBatchersFactory requestBatchersFactory =
                RequestBatchersFactory.create(lockWatchCache, Namespace.of(namespace), requestBatcherProvider.get());

        return ImmutableTimeLockHelperServices.builder()
                .lockWatchManager(lockWatchManager)
                .requestBatchersFactory(requestBatchersFactory)
                .build();
    }
}
