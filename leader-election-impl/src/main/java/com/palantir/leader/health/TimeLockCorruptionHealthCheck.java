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

package com.palantir.leader.health;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.corruption.TimeLockCorruptionPinger;

public class TimeLockCorruptionHealthCheck {
    private final ScheduledExecutorService executor = PTExecutors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("corruption-detector", true));
    private final List<TimeLockCorruptionPinger> corruptionPingers;
    private final TimeLockCorruptionPinger localCorruptionPinger;
    private boolean corruptionOnRemote = false;
    private boolean corruptionOnLocal = false;

    public TimeLockCorruptionHealthCheck(
            TimeLockCorruptionPinger localCorruptionPinger,
            List<TimeLockCorruptionPinger> corruptionPingers) {
        this.localCorruptionPinger = localCorruptionPinger;
        this.corruptionPingers = corruptionPingers;
        scheduleWithFixedDelay();
    }

    private void scheduleWithFixedDelay() {
        executor.scheduleWithFixedDelay(() -> {
                    if (detectedSignsOfCorruption()) {
                        localHasDetectedCorruption();
                    }
                },
                300,
                300,
                TimeUnit.SECONDS);
    }

    private boolean detectedSignsOfCorruption() {
        //todo Sudiksha
        return false;
    }

    public boolean isHealthy() {
        return this.corruptionOnRemote && this.corruptionOnLocal;
    }

    public void remoteHasDetectedCorruption() {
        this.corruptionOnRemote = true;
    }

    public void localHasDetectedCorruption() {
        this.corruptionOnLocal = true;
    }

    private boolean failByPagingAtlasDBAndStoppingCluster() {
        localCorruptionPinger.shutDown();
        corruptionPingers.forEach(pinger -> pinger.shutDown());
        return true;
    }
}
