/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.watch;

import com.palantir.lock.watch.LockWatchInfo.State;
import java.util.OptionalLong;

public final class LockWatchInfos {
    private LockWatchInfos() {}

    /**
     * Used to denote lock watch info for locks that are not watched, or more generally, for locks for which the state
     * is currently unknown.
     */
    public static final LockWatchInfo UNKNOWN = ImmutableLockWatchInfo.of(State.NOT_WATCHED, OptionalLong.empty());
}
