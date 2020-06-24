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

import java.util.function.Function;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.timelock.adjudicate.feedback.TimeLockClientFeedbackService;
import com.palantir.atlasdb.timelock.adjudicate.feedback.TimeLockClientFeedbackServiceEndpoints;
import com.palantir.atlasdb.timelock.adjudicate.feedback.UndertowTimeLockClientFeedbackService;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.paxos.Client;
import com.palantir.timelock.feedback.ConjureTimeLockClientFeedback;
import com.palantir.tokens.auth.AuthHeader;

public class TimeLockClientFeedbackResource implements UndertowTimeLockClientFeedbackService {
    private Function<Client, Boolean> leadershipCheck;

    private TimeLockClientFeedbackResource(Function<Client, Boolean> leadershipCheck) {
        this.leadershipCheck = leadershipCheck;
    }

    public static TimeLockClientFeedbackResource create(Function<Client, Boolean> leadershipCheck) {
        return new TimeLockClientFeedbackResource(leadershipCheck);
    }

    public static UndertowService undertow(Function<Client, Boolean> leadershipCheck) {
        return TimeLockClientFeedbackServiceEndpoints.of(TimeLockClientFeedbackResource.create(leadershipCheck));
    }

    public static TimeLockClientFeedbackService jersey(Function<Client, Boolean> leadershipCheck) {
        return new JerseyAdapter(TimeLockClientFeedbackResource.create(leadershipCheck));
    }
    @Override
    public ListenableFuture<Void> reportFeedback(AuthHeader authHeader, ConjureTimeLockClientFeedback feedbackReport) {
        // todo - this will not work for Alta
        if (leadershipCheck.apply(Client.of(feedbackReport.getServiceName()))) {
            // process feedback
        }
        return Futures.immediateVoidFuture();
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
