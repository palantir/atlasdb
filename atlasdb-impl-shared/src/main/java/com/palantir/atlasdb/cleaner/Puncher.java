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

import java.util.function.Supplier;

public interface Puncher {

    /**
     * Used for Punchers that can be initialized asynchronously (i.e. those extending
     * {@link com.palantir.async.initializer.AsyncInitializer}; other Punchers can keep the default implementation,
     * and return true (they're trivially fully initialized).
     *
     * @return true if and only if the Puncher has been initialized
     */
    default boolean isInitialized() {
        return true;
    }

    /**
     * Indicate the given timestamp has just been created.
     */
    void punch(long timestamp);

    /**
     * Returns a timestamp smaller than the timestamp of any currently un-timed-out transaction.
     */
    Supplier<Long> getTimestampSupplier();

    /**
     * Releases resources associated with this Puncher (usually means thread pools).
     */
    void shutdown();
}
