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

class HeldImmutableTsToken implements LockToken {
    private UUID requestId;
    private SharedImmutableTsToken sharedImmutableTsToken;
    private volatile boolean invalidated;

    @Override
    public UUID getRequestId() {
        return requestId;
    }

    //adding without marking?
    HeldImmutableTsToken(SharedImmutableTsToken sharedImmutableTsToken, UUID id) {
        this.requestId = id;
        this.sharedImmutableTsToken = sharedImmutableTsToken;
        this.invalidated = false;
    }

    static HeldImmutableTsToken of(SharedImmutableTsToken sharedImmutableTsToken) {
        sharedImmutableTsToken.mark();
        return new HeldImmutableTsToken(sharedImmutableTsToken, UUID.randomUUID());
    }

    synchronized void invalidate() {
        if (!invalidated) {
            invalidated = true;
            sharedImmutableTsToken.unmark();
        }
    }

    SharedImmutableTsToken sharedToken() {
        return sharedImmutableTsToken;
    }
}
