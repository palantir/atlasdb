/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.transaction.timestamp;

import java.util.UUID;

import com.palantir.atlasdb.timelock.paxos.ManagedTimestampService;

/**
 * Like {@link com.palantir.timestamp.TimestampService}, but may provide timestamps that are tailored to client
 * requirements.
 */
public interface ClientAwareManagedTimestampService extends ManagedTimestampService {
    /**
     * Returns a fresh timestamp that is suitable for use by the client with the provided identifier.
     *
     * A {@link ClientAwareManagedTimestampService} maintains the same guarantees as a
     * {@link com.palantir.timestamp.TimestampService} in terms of timestamp freshness; that is,
     * a request to this method should return a timestamp greater than any timestamp
     * that may have been observed (for any client identifier) before the request was initiated.
     *
     * @param clientIdentifier UUID identifying the client; should be consistent across the client's lifetime
     * @return a suitable timestamp
     */
    long getFreshTimestampForClient(UUID clientIdentifier);
}
