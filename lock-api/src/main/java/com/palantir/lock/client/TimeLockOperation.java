/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client;

import org.immutables.value.Value;

@Value.Immutable
public interface TimeLockOperation {
    Type type();

    Object[] arguments(); // boo!

    // TODO (jkong): Danger, Will Robinson. Here be dragons. One does not simply use this class.
    // TODO (jkong): Actually use the type system.
    enum Type {
        REFRESH_V1, // Set<UUID>
        REFRESH_V2, // Set<UUID>
        UNLOCK_V1, // Set<UUID>
        UNLOCK_V2, // Set<UUID>
        FRESH_TIMESTAMP,
        FRESH_TIMESTAMPS, // long number
        LEADER_TIME,
        GET_COMMIT_TIMESTAMPS, // GetCommitTimestampsRequest
        START_TRANSACTIONS // ConjureStartTranasctionsRequest
    }
}
