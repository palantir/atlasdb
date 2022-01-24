/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.paxos;

import java.util.function.Function;

public final class WrappedPaxosResponse<T> implements PaxosResponse {
    private final boolean isSuccessful;
    private final T response;

    public WrappedPaxosResponse(boolean isSuccessful, T response) {
        this.response = response;
        this.isSuccessful = isSuccessful;
    }

    public WrappedPaxosResponse(Function<T, Boolean> successEvaluator, T response) {
        this.response = response;
        this.isSuccessful = successEvaluator.apply(response);
    }

    @Override
    public boolean isSuccessful() {
        return isSuccessful;
    }

    public T getResponse() {
        return response;
    }
}
