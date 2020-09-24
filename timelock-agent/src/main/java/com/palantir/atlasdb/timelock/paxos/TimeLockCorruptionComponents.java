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

package com.palantir.atlasdb.timelock.paxos;

import java.util.List;

import org.immutables.value.Value;

import com.palantir.atlasdb.timelock.corruption.CorruptionHealthCheck;
import com.palantir.atlasdb.timelock.corruption.RemoteCorruptionDetector;
import com.palantir.timelock.history.TimeLockPaxosHistoryProvider;

@Value.Immutable
public interface TimeLockCorruptionComponents {
    CorruptionHealthCheck timeLockCorruptionHealthCheck();
    RemoteCorruptionDetector remoteCorruptionDetector();
    List<TimeLockPaxosHistoryProvider> remoteHistoryProviders();

    static ImmutableTimeLockCorruptionComponents.Builder builder() {
        return ImmutableTimeLockCorruptionComponents.builder();
    }
}
