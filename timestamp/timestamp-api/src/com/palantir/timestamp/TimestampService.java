// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.timestamp;

public interface TimestampService {
    /**
     * This will get a fresh timestamp that is guaranteed to be newer than any other timestamp
     * handed out before this method was called.
     */
    long getFreshTimestamp();

    /**
     * @return never null. Inclusive LongRange of timestamps.  Will always have at least 1 value.
     * This range may have less than the requested amount.
     */
    TimestampRange getFreshTimestamps(int numTimestampsRequested);

    /**
     * @return True if and only if this service is fetching timestamps from the database matching
     * this identifier.
     */
    boolean isRunningAgainstExpectedDatabase(DatabaseIdentifier id);

    /**
     * @return True if this service is in sync with its backing store.
     */
    boolean isTimestampStoreStillValid();
}
