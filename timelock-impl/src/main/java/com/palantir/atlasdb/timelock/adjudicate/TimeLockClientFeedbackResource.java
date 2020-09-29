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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.timelock.adjudicate.feedback.TimeLockClientFeedbackService;
import com.palantir.atlasdb.timelock.adjudicate.feedback.TimeLockClientFeedbackServiceEndpoints;
import com.palantir.atlasdb.timelock.adjudicate.feedback.UndertowTimeLockClientFeedbackService;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.paxos.Client;
import com.palantir.timelock.feedback.ConjureTimeLockClientFeedback;
import com.palantir.tokens.auth.AuthHeader;
import java.util.function.Predicate;

public class TimeLockClientFeedbackResource implements UndertowTimeLockClientFeedbackService {
    private Predicate<Client> leadershipCheck;
    private FeedbackHandler feedbackHandler;

    private TimeLockClientFeedbackResource(
            FeedbackHandler feedbackHandler,
            Predicate<Client> leadershipCheck) {
        this.feedbackHandler = feedbackHandler;
        this.leadershipCheck = leadershipCheck;
    }

    public static TimeLockClientFeedbackResource create(
            FeedbackHandler feedbackHandler,
            Predicate<Client> leadershipCheck) {
        return new TimeLockClientFeedbackResource(feedbackHandler, leadershipCheck);
    }

    public static UndertowService undertow(
            FeedbackHandler feedbackHandler,
            Predicate<Client> leadershipCheck) {
        return TimeLockClientFeedbackServiceEndpoints.of(TimeLockClientFeedbackResource.create(
                feedbackHandler,
                leadershipCheck));
    }

    public static TimeLockClientFeedbackService jersey(
            FeedbackHandler feedbackHandler,
            Predicate<Client> leadershipCheck) {
        return new JerseyAdapter(TimeLockClientFeedbackResource.create(feedbackHandler, leadershipCheck));
    }

    @Override
    public ListenableFuture<Void> reportFeedback(AuthHeader authHeader, ConjureTimeLockClientFeedback feedbackReport) {
        if (leadershipCheck.test(getClient(feedbackReport))) {
            feedbackHandler.handle(feedbackReport);
        }
        return Futures.immediateVoidFuture();
    }

    public Client getClient(ConjureTimeLockClientFeedback feedbackReport) {
        return Client.of(feedbackReport.getNamespace().orElseGet(() -> feedbackReport.getServiceName()));
    }

    public static final class JerseyAdapter implements TimeLockClientFeedbackService {
        private final TimeLockClientFeedbackResource delegate;

        private JerseyAdapter(TimeLockClientFeedbackResource timeLockClientFeedbackResource) {
            this.delegate = timeLockClientFeedbackResource;
        }

        @Override
        public void reportFeedback(AuthHeader authHeader, ConjureTimeLockClientFeedback feedbackReport) {
            delegate.reportFeedback(authHeader, feedbackReport);
        }
    }

}
