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

package com.palantir.atlasdb.cleaner;

/**
 * This is the underlying store used by the puncher for keeping track in a persistent way of the
 * wall-clock/timestamp mapping.
 *
 * @author jweel
 */
public interface PuncherStore {
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
}
