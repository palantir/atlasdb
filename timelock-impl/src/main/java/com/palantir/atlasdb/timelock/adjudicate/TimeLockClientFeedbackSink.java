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

package com.palantir.atlasdb.timelock.adjudicate;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.timelock.feedback.ConjureTimeLockClientFeedback;
import java.util.List;
import java.util.UUID;

public final class TimeLockClientFeedbackSink {
    private final Cache<UUID, ConjureTimeLockClientFeedback> trackedFeedbackReports;

    private TimeLockClientFeedbackSink(Cache<UUID, ConjureTimeLockClientFeedback> trackedFeedbackReports) {
        this.trackedFeedbackReports = trackedFeedbackReports;
    }

    static TimeLockClientFeedbackSink createAndInstrument(
            MetricsManager metricsManager, Cache<UUID, ConjureTimeLockClientFeedback> trackedFeedbackReports) {
        metricsManager.registerOrGetGauge(
                TimeLockClientFeedbackSink.class,
                "numberOfFeedbackReports",
                () -> trackedFeedbackReports::estimatedSize);
        return create(trackedFeedbackReports);
    }

    public static TimeLockClientFeedbackSink create(Cache<UUID, ConjureTimeLockClientFeedback> trackedFeedbackReports) {
        return new TimeLockClientFeedbackSink(trackedFeedbackReports);
    }

    public void registerFeedback(ConjureTimeLockClientFeedback feedback) {
        trackedFeedbackReports.put(UUID.randomUUID(), feedback);
    }

    public List<ConjureTimeLockClientFeedback> getTrackedFeedbackReports() {
        return ImmutableList.copyOf(trackedFeedbackReports.asMap().values());
    }
}
