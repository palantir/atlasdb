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

package com.palantir.atlasdb.timelock.corruption;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.timelock.corruption.TimeLockCorruptionPinger;
import com.palantir.tokens.auth.AuthHeader;

public class TimeLockLocalCorruptionDetector {
    private static final Logger log = LoggerFactory.getLogger(
            TimeLockLocalCorruptionDetector.class);

    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer omitted");
    private static final Duration TIMELOCK_CORRUPTION_ANALYSIS_INTERVAL = Duration.ofMinutes(5);
    private static final String CORRUPTION_DETECTOR_THREAD_PREFIX = "timelock-corruption-detector";

    private final ScheduledExecutorService executor = PTExecutors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory(CORRUPTION_DETECTOR_THREAD_PREFIX, true));
    private final List<TimeLockCorruptionPinger> corruptionPingers;

    private TimeLockCorruptionStatus localCorruptionState = TimeLockCorruptionStatus.HEALTHY;

    public static TimeLockLocalCorruptionDetector create(List<TimeLockCorruptionPinger> corruptionPingers) {
        TimeLockLocalCorruptionDetector timeLockLocalCorruptionDetector
                = new TimeLockLocalCorruptionDetector(corruptionPingers);

//        TODO(snanda) - uncomment when TL corruption detection goes live
//        timeLockLocalCorruptionDetector.scheduleWithFixedDelay();
        return timeLockLocalCorruptionDetector;
    }

    private TimeLockLocalCorruptionDetector(List<TimeLockCorruptionPinger> corruptionPingers) {
        this.corruptionPingers = corruptionPingers;
    }

    private void scheduleWithFixedDelay() {
        executor.scheduleWithFixedDelay(() -> {
                    if (detectedSignsOfCorruption()) {
                        killTimeLock();
                    }
                },
                TIMELOCK_CORRUPTION_ANALYSIS_INTERVAL.getSeconds(),
                TIMELOCK_CORRUPTION_ANALYSIS_INTERVAL.getSeconds(),
                TimeUnit.SECONDS);
    }

    private void killTimeLock() {
        corruptionPingers.forEach(this::reportCorruptionToRemote);
        localDetectedCorruption();
    }

    private void localDetectedCorruption() {
        localCorruptionState = TimeLockCorruptionStatus.CORRUPTION;
    }

    private void reportCorruptionToRemote(TimeLockCorruptionPinger pinger) {
        try {
            pinger.corruptionDetected(AUTH_HEADER);
        } catch (Exception e) {
            log.warn("Failed to report corruption to remote.", e);
        }
    }

    public TimeLockCorruptionStatus getLocalCorruptionState() {
        return localCorruptionState;
    }

    private boolean detectedSignsOfCorruption() {
        // no op for now
        return false;
    }
}
