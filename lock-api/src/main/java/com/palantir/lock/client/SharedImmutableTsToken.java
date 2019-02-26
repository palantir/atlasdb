/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import java.util.UUID;

import com.palantir.lock.v2.LockToken;

class SharedImmutableTsToken implements LockToken {
    private LockToken token;
    private volatile int reference;

    SharedImmutableTsToken(LockToken token) {
        this.token = token;
        reference = 0;
    }

    synchronized void mark() {
        reference++;
    }

    synchronized void unmark() {
        //precondition
        reference--;
    }

    synchronized boolean dereferenced() {
        return reference == 0;
    }

    LockToken token() {
        return token;
    }

    @Override
    public UUID getRequestId() {
        return token.getRequestId();
    }
}
