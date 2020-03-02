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

package com.palantir.common.time;

import static com.palantir.logsafe.Preconditions.checkState;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

public enum NanoClock implements Supplier<NanoTime> {
    INSTANCE;

    private static final AtomicReferenceFieldUpdater<NanoClock, NanoTime> updater =
            AtomicReferenceFieldUpdater.newUpdater(NanoClock.class, NanoTime.class, "maxSeen");
    private volatile NanoTime maxSeen;

    @Override
    public NanoTime get() {
        NanoTime currentMax = maxSeen;
        NanoTime now = NanoTime.now();
        checkState(currentMax == null || !now.isBefore(currentMax), "Clock reversal detected");
        maybeSetNewMax(now);
        return now;
    }

    private void maybeSetNewMax(NanoTime time) {
        while (true) {
            NanoTime current = maxSeen;
            if ((current != null && time.isBefore(current)) || updater.compareAndSet(this, current, time)) {
                return;
            }
        }
    }
}
