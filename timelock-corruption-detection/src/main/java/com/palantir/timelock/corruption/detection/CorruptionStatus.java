/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.corruption.detection;

public enum CorruptionStatus {
    HEALTHY(false, false),
    DIVERGED_LEARNERS(true, false), // this is false for now
    VALUE_LEARNED_WITHOUT_QUORUM(true, false),
    ACCEPTED_VALUE_GREATER_THAN_LEARNED(true, false),
    DEFINITIVE_CORRUPTION_DETECTED_BY_LOCAL(true, true),
    DEFINITIVE_CORRUPTION_DETECTED_BY_REMOTE(true, true);

    private final boolean shouldRaiseErrorAlert;
    private final boolean shouldShootTimeLock;

    CorruptionStatus(boolean shouldRaiseErrorAlert, boolean shouldShootTimeLock) {
        this.shouldRaiseErrorAlert = shouldRaiseErrorAlert;
        this.shouldShootTimeLock = shouldShootTimeLock;
    }

    public boolean shootTimeLock() {
        return shouldShootTimeLock;
    }

    public boolean raiseErrorAlert() {
        return shouldRaiseErrorAlert;
    }
}
