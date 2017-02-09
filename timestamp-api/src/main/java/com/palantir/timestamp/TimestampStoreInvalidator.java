/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.timestamp;

public interface TimestampStoreInvalidator {
    /**
     * This method corrupts the data in an underlying timestamp bound store. This is important for the safety of
     * timestamp store migrations; if one migrates AtlasDB to a different timestamp bound store, then the
     * original timestamp bound store must not be used again if it is not updated with information about what
     * timestamps the new store has given out.
     *
     * As this method performs corruption, it should only be called *after* the migration of the existing timestamp
     * bound to the new timestamp bound store has been carried out successfully.
     *
     * Of course, this is not strictly needed if no one ever uses the original timestamp bound store again.
     * However, we find this to be a safety precaution worth taking, especially if AtlasDB is subsequently downgraded.
     */
    void invalidateTimestampStore();

    /**
     * This method performs the inverse of invalidateTimestampStore().
     *
     * It is not safe to use a revalidated timestamp store directly, *unless* we can guarantee that no timestamps
     * have been taken out for the relevant data since invalidation.
     */
    default void revalidateTimestampStore() {
        throw new UnsupportedOperationException("This timestamp store does not support revalidation!");
    }
}
