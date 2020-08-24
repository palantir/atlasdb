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

import com.palantir.corruption.TimeLockCorruptionPinger;

public class TimeLockCorruptionDetectionHealthCheck {
    private final List<TimeLockCorruptionPinger> corruptionPingers;
    private final TimeLockCorruptionPinger localCorruptionPinger;

    public TimeLockCorruptionDetectionHealthCheck(
            TimeLockCorruptionPinger localCorruptionPinger,
            List<TimeLockCorruptionPinger> corruptionPingers) {
        this.localCorruptionPinger = localCorruptionPinger;
        this.corruptionPingers = corruptionPingers;
    }


    public boolean isHealthy() {
        return true;
    }

    public boolean shouldPageAtlasDBAsCorruptionWasDetected() {
        // this is pull; is there a way to push this? is that required?
        return false;
    }

    public boolean failByPagingAtlasDBAndStoppingCluster() {
        localCorruptionPinger.shutDown();
        corruptionPingers.forEach(pinger -> pinger.shutDown());
        return true;
    }
}
