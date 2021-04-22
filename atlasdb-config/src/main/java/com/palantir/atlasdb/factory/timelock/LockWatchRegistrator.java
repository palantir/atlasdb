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

package com.palantir.atlasdb.factory.timelock;

import com.palantir.async.initializer.CallbackInitializable;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.watch.LockWatchReferences.LockWatchReference;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LockWatchRegistrator implements AutoCloseable, CallbackInitializable<TransactionManager> {
    private static final Logger log = LoggerFactory.getLogger(LockWatchRegistrator.class);

    private final ScheduledExecutorService executor = PTExecutors.newSingleThreadScheduledExecutor();
    private final Set<LockWatchReference> lockWatches;

    private LockWatchRegistrator(Set<LockWatchReference> lockWatches) {
        this.lockWatches = lockWatches;
    }

    public static Optional<LockWatchRegistrator> maybeCreate(Set<Schema> schemas) {
        Set<LockWatchReference> references = schemas.stream()
                .map(Schema::getLockWatches)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
        if (references.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new LockWatchRegistrator(references));
    }

    @Override
    public void initialize(TransactionManager resource) {
        executor.scheduleAtFixedRate(() -> registerWatches(resource), 0, 2, TimeUnit.MINUTES);
    }

    private void registerWatches(TransactionManager resource) {
        try {
            resource.getLockWatchManager().registerPreciselyWatches(lockWatches);
        } catch (Throwable t) {
            log.warn("Failed to register lock watches", t);
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
