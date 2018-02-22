/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.cleaner;

/**
 * This is the underlying store used by the puncher for keeping track in a persistent way of the
 * wall-clock/timestamp mapping.
 *
 * @author jweel
 */
public interface PuncherStore {
    /**
     * Used for PuncherStores that can be initialized asynchronously (i.e. those extending
     * {@link com.palantir.async.initializer.AsyncInitializer}; other PuncherStores can keep the default implementation,
     * and return true (they're trivially fully initialized).

     * @return true if and only if the PuncherStore has been initialized
     */
    default boolean isInitialized() {
        return true;
    }

    /**
     * Declare that timestamp was acquired at time timeMillis.  Note
     * that timestamp corresponds to "start timestamps" in the AtlasDB
     * transaction protocol.
     */
    void put(long timestamp, long timeMillis);

    /**
     * Find the latest timestamp created at or before timeMillis.
     */
    Long get(Long timeMillis);

    /**
     * Get the time in millis for the greatest timestamp punched less than or equal to the given timestamp.
     */
    long getMillisForTimestamp(long timestamp);
}
