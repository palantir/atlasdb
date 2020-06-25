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

import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.palantir.timelock.feedback.ConjureTimeLockClientFeedback;

public class FeedbackHandler {

    private final TimeLockClientFeedbackSink timeLockClientFeedbackSink = TimeLockClientFeedbackSink
            .create(Caffeine
            .newBuilder()
            .expireAfterWrite(Constants.HEALTH_FEEDBACK_REPORT_EXPIRATION_MINUTES, TimeUnit.MINUTES)
                        .build());

    public HealthStatus getTimeLockHealthStatus() {
        return FeedbackProcessor.getTimeLockHealthStatus(timeLockClientFeedbackSink.getTrackedFeedbackReports());
    }

    public void handle(ConjureTimeLockClientFeedback feedback) {
        timeLockClientFeedbackSink.registerFeedback(feedback);
    }
}
