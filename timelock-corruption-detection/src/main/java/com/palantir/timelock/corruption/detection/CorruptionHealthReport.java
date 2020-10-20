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

import java.util.Optional;

import org.immutables.value.Value;

@Value.Immutable
public interface CorruptionHealthReport {
    CorruptionStatus status();

    Optional<String> message();

    static ImmutableCorruptionHealthReport.Builder builder() {
        return ImmutableCorruptionHealthReport.builder();
    }

    static CorruptionHealthReport defaultHealthyReport() {
        return CorruptionHealthReport.builder().status(CorruptionStatus.HEALTHY).build();
    }

    static CorruptionHealthReport defaultRemoteCorruptionReport() {
        return CorruptionHealthReport.builder().status(CorruptionStatus.DEFINITIVE_CORRUPTION).build();
    }

    default CorruptionHealthReport overrideIfAllowed(CorruptionHealthReport override) {
        return this;
    }

    default boolean shootTimeLock() {
        return status().shootTimeLock();
    }

    default boolean raiseErrorAlert() {
        return status().raiseErrorAlert();
    }
}
