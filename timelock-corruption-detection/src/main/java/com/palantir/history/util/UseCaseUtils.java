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

package com.palantir.history.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.history.models.AcceptorUseCase;
import com.palantir.history.models.ImmutableAcceptorUseCase;
import com.palantir.history.models.ImmutableLearnerUseCase;
import com.palantir.history.models.LearnerUseCase;
import com.palantir.logsafe.SafeArg;

public final class UseCaseUtils {
    private static final Logger log = LoggerFactory.getLogger(UseCaseUtils.class);

    private UseCaseUtils() {
        // no op
    }

    public static String getPaxosUseCasePrefix(String useCase) {
        int delimiterIndex = useCase.indexOf("!");
        if (delimiterIndex == -1) {
            log.warn("The useCase - {} unexpectedly does not have `!` as delimiter. We should never reach here.",
                    SafeArg.of("useCase", useCase));
        }
        return useCase.substring(0, delimiterIndex);
    }

    public static LearnerUseCase getLearnerUseCase(String paxosUseCase) {
        return ImmutableLearnerUseCase.of(String.format("%s!learner", paxosUseCase));
    }

    public static AcceptorUseCase getAcceptorUseCase(String paxosUseCase) {
        return ImmutableAcceptorUseCase.of(String.format("%s!acceptor", paxosUseCase));
    }
}
