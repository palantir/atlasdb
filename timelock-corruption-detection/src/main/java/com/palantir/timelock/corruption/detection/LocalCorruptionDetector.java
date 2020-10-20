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

import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.timelock.corruption.TimeLockCorruptionNotifier;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class LocalCorruptionDetector implements CorruptionDetector {
    private static final Duration TIMELOCK_CORRUPTION_ANALYSIS_INTERVAL = Duration.ofMinutes(5);
    private static final String CORRUPTION_DETECTOR_THREAD_PREFIX = "timelock-corruption-detector";

    private final ScheduledExecutorService executor = PTExecutors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory(CORRUPTION_DETECTOR_THREAD_PREFIX, true));
    private final LocalCorruptionHandler corruptionHandler;

    private volatile CorruptionStatus localCorruptionState = CorruptionStatus.HEALTHY;

    public static LocalCorruptionDetector create(List<TimeLockCorruptionNotifier> corruptionNotifiers) {
        LocalCorruptionDetector localCorruptionDetector = new LocalCorruptionDetector(corruptionNotifiers);

        //        TODO(snanda) - uncomment when TL corruption detection goes live
        //        timeLockLocalCorruptionDetector.scheduleWithFixedDelay();
        return localCorruptionDetector;
    }

    private LocalCorruptionDetector(List<TimeLockCorruptionNotifier> corruptionNotifiers) {
        this.corruptionHandler = new LocalCorruptionHandler(corruptionNotifiers);
    }

    private void scheduleWithFixedDelay() {
        executor.scheduleWithFixedDelay(
                () -> {
                    if (detectedSignsOfCorruption()) {
                        killTimeLock();
                    }
                },
                TIMELOCK_CORRUPTION_ANALYSIS_INTERVAL.getSeconds(),
                TIMELOCK_CORRUPTION_ANALYSIS_INTERVAL.getSeconds(),
                TimeUnit.SECONDS);
    }

    private void killTimeLock() {
        localCorruptionState = CorruptionStatus.CORRUPTION;
        corruptionHandler.notifyRemoteServersOfCorruption();
    }

    private boolean detectedSignsOfCorruption() {
        // no op for now
        return false;
    }

    @Override
    public boolean hasDetectedCorruption() {
        return localCorruptionState.hasCorruption();
    }
}
