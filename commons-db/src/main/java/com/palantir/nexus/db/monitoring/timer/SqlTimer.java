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
package com.palantir.nexus.db.monitoring.timer;

import com.palantir.nexus.db.sql.BasicSQLString;

/**
 * Code extracted from {@link BasicSQLString}. This provides one of several competing and
 * overlapping mechanisms to register monitoring callbacks for various SQL actions.
 *
 * Class for tracking timing information for a single SQL call. An instance of {@link Handle} should
 * be constructed before making the call, and {@link Handle#stop()} should be called after the call
 * completes.
 */
public interface SqlTimer {
    Handle start(String module, String sqlKey, String rawSql);

    interface Handle {
        /**
         * Called when the SQL call is complete. Updates the SQL statistics and writes timing
         * information to the timing logger.
         */
        void stop();
    }
}
