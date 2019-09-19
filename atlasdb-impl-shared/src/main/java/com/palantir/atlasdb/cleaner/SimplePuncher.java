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
package com.palantir.atlasdb.cleaner;

import com.palantir.common.time.Clock;
import java.util.function.Supplier;

/**
 * Puncher implementation that calls the underlying PuncherStore directly, and blockingly, on every
 * #punch() call it gets. This should normally be wrapped in an AsyncPuncher for performance
 * reasons.
 *
 * @author jweel
 */
public final class SimplePuncher implements Puncher {
    public static SimplePuncher create(PuncherStore puncherStore,
                                       Clock clock,
                                       Supplier<Long> transactionReadTimeoutMillisSupplier) {
        return new SimplePuncher(puncherStore, clock, transactionReadTimeoutMillisSupplier);
    }

    private final PuncherStore puncherStore;
    private final Clock clock;
    private final Supplier<Long> transactionReadTimeoutMillisSupplier;

    private SimplePuncher(PuncherStore puncherStore, Clock clock, Supplier<Long> transactionReadTimeoutMillisSupplier) {
        this.puncherStore = puncherStore;
        this.clock = clock;
        this.transactionReadTimeoutMillisSupplier = transactionReadTimeoutMillisSupplier;
    }

    @Override
    public boolean isInitialized() {
        return puncherStore.isInitialized();
    }

    @Override
    public void punch(long timestamp) {
        puncherStore.put(timestamp, clock.getTimeMillis());

    }

    @Override
    public Supplier<Long> getTimestampSupplier() {
        return () -> puncherStore.get(clock.getTimeMillis() - transactionReadTimeoutMillisSupplier.get());
    }

    @Override
    public void shutdown() {
        // Do nothing
    }
}
