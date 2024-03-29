/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock;

import org.immutables.value.Value;

@Value.Immutable
public interface LockClientAndThread {
    LockClientAndThread UNKNOWN = LockClientAndThread.of(LockClient.ANONYMOUS, "unknown-thread");

    LockClient client();

    String thread();

    static ImmutableLockClientAndThread.Builder builder() {
        return ImmutableLockClientAndThread.builder();
    }

    static LockClientAndThread of(LockClient client, String requestingThread) {
        return builder().client(client).thread(requestingThread).build();
    }
}
