/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.autobatch;

import com.google.common.base.Stopwatch;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.tracing.DetachedSpan;
import java.time.Duration;

final class TimedDetachedSpan {
    private final DetachedSpan delegate;
    private final Stopwatch stopwatch;
    private volatile boolean completed = false;

    private TimedDetachedSpan(Stopwatch stopwatch, DetachedSpan delegate) {
        this.stopwatch = stopwatch;
        this.delegate = delegate;
    }

    void complete() {
        delegate.complete();
        stopwatch.stop();
        completed = true;
    }

    Duration getDurationOrThrowIfStillRunning() {
        if (!completed) {
            throw new SafeRuntimeException("Fetching duration was attempted while the span task is still running.");
        }
        return stopwatch.elapsed();
    }

    static TimedDetachedSpan from(DetachedSpan delegate) {
        return new TimedDetachedSpan(Stopwatch.createStarted(), delegate);
    }
}
