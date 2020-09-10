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

package com.palantir.timelock.corruption;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.palantir.atlasdb.timelock.corruption.TimeLockCorruptionHealthCheck;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.tokens.auth.AuthHeader;

public final class LocalCorruptionDetector {
    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer omitted");

    private final ScheduledExecutorService executor = PTExecutors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("corruption-detector", true));
    private final TimeLockCorruptionHealthCheck healthCheck;
    private final List<TimeLockCorruptionPinger> corruptionPingers;

    public LocalCorruptionDetector(TimeLockCorruptionHealthCheck healthCheck, List<TimeLockCorruptionPinger> corruptionPingers) {
        this.healthCheck = healthCheck;
        this.corruptionPingers = corruptionPingers;
        scheduleWithFixedDelay();
    }

    private void scheduleWithFixedDelay() {
        executor.scheduleWithFixedDelay(() -> {
                    if (detectedSignsOfCorruption()) {
                        triggerCorruptionDetected();
                    }
                },
                300,
                300,
                TimeUnit.SECONDS);
    }

    private void triggerCorruptionDetected() {
        healthCheck.setLocalHasDetectedCorruption(true);
        corruptionPingers.forEach(pinger -> pinger.corruptionDetected(AUTH_HEADER));
    }

    private boolean detectedSignsOfCorruption() {
        //todo Sudiksha
        return false;
    }
}
