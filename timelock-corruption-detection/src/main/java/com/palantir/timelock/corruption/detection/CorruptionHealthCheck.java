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

import java.util.List;

import com.google.common.collect.ImmutableList;

public class CorruptionHealthCheck {
    private final LocalCorruptionDetector localCorruptionDetector;
    private final List<CorruptionDetector> localAndRemoteCorruptionDetectors;

    public CorruptionHealthCheck(LocalCorruptionDetector localCorruptionDetector,
            RemoteCorruptionDetector remoteCorruptionDetector) {
        this.localCorruptionDetector = localCorruptionDetector;
        this.localAndRemoteCorruptionDetectors = ImmutableList.of(localCorruptionDetector, remoteCorruptionDetector);
    }

    public boolean shootTimeLock() {
        return localAndRemoteCorruptionDetectors.stream()
                .anyMatch(detector -> detector.corruptionHealthReport().shootTimeLock());
    }

    public CorruptionHealthReport localCorruptionDetector() {
        //todo(snanda) should merge local and remote reports?
        return localCorruptionDetector.corruptionHealthReport();
    }
}
