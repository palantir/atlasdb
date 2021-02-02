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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.palantir.timelock.feedback.ConjureTimeLockClientFeedback;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.Test;

public class FeedbackSinkTest {
    private static final FakeTicker FAKE_TICKER = new FakeTicker();
    private static final ConjureTimeLockClientFeedback TEST_REPORT = getTestReport();

    @Test
    public void feedbackIsRegistered() {
        List<ConjureTimeLockClientFeedback> trackedFeedbackReports =
                createSinkAndAddTestReport().getTrackedFeedbackReports();
        assertThat(trackedFeedbackReports).hasSize(1);
    }

    @Test
    public void multipleFeedbackReportsAreRegistered() {
        TimeLockClientFeedbackSink sink = createSinkAndAddTestReport();
        IntStream.range(1, 100).forEach(index -> registerTestReport(sink));
        List<ConjureTimeLockClientFeedback> trackedFeedbackReports = sink.getTrackedFeedbackReports();
        assertThat(trackedFeedbackReports).hasSize(100);

        FAKE_TICKER.advance(Constants.HEALTH_FEEDBACK_REPORT_EXPIRATION_MINUTES.toMinutes(), TimeUnit.MINUTES);
        assertThat(sink.getTrackedFeedbackReports()).isEmpty();
    }

    @Test
    public void feedbackRetrievedFromSinkIsImmutable() {
        List<ConjureTimeLockClientFeedback> trackedFeedbackReports =
                createSinkAndAddTestReport().getTrackedFeedbackReports();
        assertThatThrownBy(() -> trackedFeedbackReports.add(TEST_REPORT))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void feedbackReportIsEvictedAfterExpiry() {
        TimeLockClientFeedbackSink sink = createSinkAndAddTestReport();
        List<ConjureTimeLockClientFeedback> trackedFeedbackReports = sink.getTrackedFeedbackReports();
        assertThat(trackedFeedbackReports).hasSize(1);

        FAKE_TICKER.advance(Constants.HEALTH_FEEDBACK_REPORT_EXPIRATION_MINUTES.toMinutes(), TimeUnit.MINUTES);
        assertThat(sink.getTrackedFeedbackReports()).isEmpty();
    }

    TimeLockClientFeedbackSink createSinkAndAddTestReport() {
        TimeLockClientFeedbackSink timeLockClientFeedbackSink = TimeLockClientFeedbackSink.create(Caffeine.newBuilder()
                .expireAfterWrite(Constants.HEALTH_FEEDBACK_REPORT_EXPIRATION_MINUTES)
                .ticker(FAKE_TICKER)
                .build());
        registerTestReport(timeLockClientFeedbackSink);
        return timeLockClientFeedbackSink;
    }

    private void registerTestReport(TimeLockClientFeedbackSink sink) {
        sink.registerFeedback(getTestReport());
    }

    static ConjureTimeLockClientFeedback getTestReport() {
        return ConjureTimeLockClientFeedback.builder()
                .nodeId(UUID.randomUUID())
                .serviceName("client_1")
                .atlasVersion("0.1.0")
                .build();
    }
}
