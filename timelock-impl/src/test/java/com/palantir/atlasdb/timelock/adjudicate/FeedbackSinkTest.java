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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.palantir.timelock.feedback.ConjureTimeLockClientFeedback;

public class FeedbackSinkTest {
    private static final ConjureTimeLockClientFeedback TEST_REPORT = getTestReport();

    static {
        TimeLockClientFeedbackSink.registerFeedback(TEST_REPORT);
    }

    @Test
    public void feedbackIsRegistered() {
        List<ConjureTimeLockClientFeedback> trackedFeedbackReports =
                TimeLockClientFeedbackSink.getTrackedFeedbackReports();
        assertThat(trackedFeedbackReports.size()).isEqualTo(1);
        assertThat(trackedFeedbackReports).containsExactly(TEST_REPORT);
    }

    @Test
    public void feedbackRetrievedFromSinkIsImmutable() {
        List<ConjureTimeLockClientFeedback> trackedFeedbackReports =
                TimeLockClientFeedbackSink.getTrackedFeedbackReports();
        assertThatThrownBy(() -> trackedFeedbackReports.add(TEST_REPORT))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    static ConjureTimeLockClientFeedback getTestReport() {
        return ConjureTimeLockClientFeedback.builder()
                .nodeId(UUID.randomUUID())
                .serviceName("client_1")
                .atlasVersion("0.1.0")
                .build();
    }
}
