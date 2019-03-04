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
package com.palantir.timestamp;

public interface TimestampStoreInvalidator {
    /**
     * This method performs a backup of the data in an underlying timestamp bound store, and then corrupts it
     * (if this has not already been performed). It then returns the value that has been backed up.
     * These operations are grouped because of the requirement that as far as the timestamp bound store is concerned,
     * this operation should be atomic. Otherwise we can have the following situation:
     *
     * 1. Client A reads a backup timestamp B1, preparing to invalidate and backup B1
     * 2. Client B issues the timestamp B1 + 1
     * 3. Client A invalidates the internal store and fast forwards a different timestamp service to B1
     * 4. Client B crashes (fine, it can't issue any more timestamps)
     * 5. Client A now issues the timestamp B1 + 1 because of the external timestamp service
     *
     * Invalidation is important for the safety of timestamp store migrations; if one migrates AtlasDB to a different
     * timestamp bound store, then the original timestamp bound store must not be used again if it is not updated with
     * information about what timestamps the new store has given out.
     *
     * Of course, this is not strictly needed if no one ever uses the original timestamp bound store again.
     * However, we find this to be a safety precaution worth taking, especially if AtlasDB is subsequently downgraded.
     *
     * @return the value of the bound that is backed up
     */
    long backupAndInvalidate();

    /**
     * This method performs the inverse of backupAndInvalidate().
     *
     * It is not safe to use a revalidated timestamp store directly, *unless* we can guarantee that no timestamps
     * have been taken out for the relevant data since invalidation.
     */
    default void revalidateFromBackup() {
        throw new UnsupportedOperationException("Restore is not supported by this timestamp store.");
    }
}
