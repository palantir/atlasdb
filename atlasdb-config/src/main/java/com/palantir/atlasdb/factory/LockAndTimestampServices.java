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

import com.palantir.atlasdb.factory.startup.TimeLockMigrator;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerInternal;
import com.palantir.atlasdb.keyvalue.api.watch.NoOpLockWatchManager;
import com.palantir.common.annotations.ImmutablesStyles.NoStagedBuilderStyle;
import com.palantir.lock.LockService;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.DelegatingManagedTimestampService;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@NoStagedBuilderStyle
public interface LockAndTimestampServices {
    LockService lock();

    TimestampService timestamp();

    TimestampManagementService timestampManagement();

    TimelockService timelock();

    Optional<TimeLockMigrator> migrator();

    @Value.Default
    default LockWatchManagerInternal lockWatcher() {
        return NoOpLockWatchManager.create();
    }

    @Value.Derived
    default ManagedTimestampService managedTimestampService() {
        return new DelegatingManagedTimestampService(timestamp(), timestampManagement());
    }

    @SuppressWarnings("checkstyle:WhitespaceAround")
    @Value.Default
    default List<Runnable> resources() {
        return Collections.emptyList();
    }
}
