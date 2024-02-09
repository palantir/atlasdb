/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.paxos;

import com.palantir.conjure.java.api.errors.ErrorType;
import com.palantir.conjure.java.api.errors.ServiceException;
import com.palantir.logsafe.SafeArg;

public final class UseCaseAwareBatchPaxosErrors {
    private static ErrorType PAXOS_USE_CASE_NOT_FOUND =
            ErrorType.create(ErrorType.Code.NOT_FOUND, "UseCaseAwareBatchPaxosAcceptorResource:PaxosUseCaseNotFound");

    public static ServiceException invalidPaxosUseCaseException(PaxosUseCase useCase) {
        return new ServiceException(PAXOS_USE_CASE_NOT_FOUND, SafeArg.of("useCase", useCase));
    }

    private UseCaseAwareBatchPaxosErrors() {}
}
