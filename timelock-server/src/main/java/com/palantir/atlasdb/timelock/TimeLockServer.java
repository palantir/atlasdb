/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock;

import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;

public interface TimeLockServer {
    /**
     * Called when the Timelock Server is started up, and is guaranteed to run before any requests
     * are accepted from clients.
     * @param configuration Timelock Server configuration; may be useful for initialisation
     */
    void onStartup(TimeLockServerConfiguration configuration);

    /**
     * Creates timestamp and lock services for the given client. It is expected that for each client there should
     * only be (up to) one active timestamp service, and one active lock service at any time.
     * @param client Client namespace to create the services for
     * @param slowLogTriggerMillis response time for lock requests that triggers slow lock logging
     * @return Invalidating timestamp and lock services
     */
    TimeLockServices createInvalidatingTimeLockServices(String client, long slowLogTriggerMillis);
}
