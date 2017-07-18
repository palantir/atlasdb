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

package com.palantir.atlasdb.cleaner;

import java.util.Optional;

class ImmutableTimestampMonitor {

    private final Long timeLimitMs;
    private final Runnable runIfFailed;

    private Optional<Long> lastImmutableTs = Optional.empty();
    private Optional<Long> lastUpdateTs = Optional.empty();
    private boolean hasPerformedAction = false;

    ImmutableTimestampMonitor(Long timeLimitMs, Runnable runIfFailed) {
        this.timeLimitMs = timeLimitMs;
        this.runIfFailed = runIfFailed;
    }

    void update(Long immutableTs) {
        if (!lastImmutableTs.isPresent() || !lastImmutableTs.get().equals(immutableTs)) {
            updateState(immutableTs);
        } else {
            checkStateExpiration();
        }
    }

    private void checkStateExpiration() {
        if (!hasPerformedAction && lastUpdateTs.isPresent()
                && System.currentTimeMillis() - lastUpdateTs.get() >= timeLimitMs) {
            hasPerformedAction = true;
            runIfFailed.run();
        }
    }

    private void updateState(Long immutableTs) {
        lastImmutableTs = Optional.ofNullable(immutableTs);
        lastUpdateTs = Optional.of(System.currentTimeMillis());
        hasPerformedAction = false;
    }
}
