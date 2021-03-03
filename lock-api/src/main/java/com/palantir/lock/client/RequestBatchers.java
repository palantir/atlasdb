/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client;

public final class RequestBatchers {
    private final MultiClientCommitTimestampGetter multiClientCommitTimestampGetter;
    private final MultiClientTransactionStarter multiClientTransactionStarter;

    public MultiClientTransactionStarter getMultiClientTransactionStarter() {
        return multiClientTransactionStarter;
    }

    public MultiClientCommitTimestampGetter getMultiClientCommitTimestampGetter() {
        return multiClientCommitTimestampGetter;
    }

    private RequestBatchers(
            MultiClientTransactionStarter multiClientTransactionStarter,
            MultiClientCommitTimestampGetter multiClientCommitTimestampGetter) {
        this.multiClientTransactionStarter = multiClientTransactionStarter;
        this.multiClientCommitTimestampGetter = multiClientCommitTimestampGetter;
    }

    public static RequestBatchers of(
            MultiClientTransactionStarter multiClientTransactionStarter,
            MultiClientCommitTimestampGetter multiClientCommitTimestampGetter) {
        return new RequestBatchers(multiClientTransactionStarter, multiClientCommitTimestampGetter);
    }
}
