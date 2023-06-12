/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock;

import java.time.Duration;
import org.immutables.value.Value;

@Value.Immutable
public interface DebugThreadInfoConfiguration {

    /** Maximum number of lock to client-thread mappings. */
    long MAX_THREAD_INFO_SIZE = 1_000_000;

    /** Time, after which mappings expire and can be deleted. */
    Duration THREAD_INFO_WRITE_EXPIRATION = Duration.ofMinutes(2);

    @Value.Default
    default boolean recordThreadInfo() {
        return false;
    }
}
