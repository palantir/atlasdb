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

package com.palantir.atlasdb.keyvalue.api.cache;

import com.palantir.atlasdb.keyvalue.api.watch.LockWatchEventCacheImpl;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.watch.LockWatchEventCache;
import org.junit.Before;

public final class LockWatchValueCacheImplTest {
    private LockWatchEventCache eventCache;
    private LockWatchValueCache valueCache;

    @Before
    public void before() {
        eventCache = LockWatchEventCacheImpl.create(MetricsManagers.createForTests());
        valueCache = new LockWatchValueCacheImpl(eventCache);
    }

    // TODO(jshah): implement these tests. It's hard to test anything yet as there is no way to get snapshots of the
    //  state (that will come when the per-transaction cache is implemented).
}
