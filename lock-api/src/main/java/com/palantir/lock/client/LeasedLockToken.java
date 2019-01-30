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

import com.palantir.lock.v2.IdentifiedTime;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockToken;

public class LeasedLockToken implements LockToken {
    private final LockToken serverToken;
    private final LockToken clientToken;

    private Lease lease;
    private volatile boolean inValidated = false;

    static LeasedLockToken of(LockToken serverToken, Lease lease) {
        return new LeasedLockToken(serverToken,
                LockToken.of(UUID.randomUUID()),
                lease);
    }

    private LeasedLockToken(LockToken serverToken, LockToken clientToken, Lease lease) {
        this.serverToken = serverToken;
        this.clientToken = clientToken;
        this.lease = lease;
    }

    LockToken serverToken() {
        return serverToken;
    }

    LockToken clientToken() {
        return clientToken;
    }

    synchronized void updateLease(Lease lease) {
        this.lease = lease;
    }

    synchronized boolean isValid(IdentifiedTime identifiedCurrentTime) {
        return !inValidated && lease.isValid(identifiedCurrentTime);
    }

    synchronized void inValidate() {
        inValidated = false;
    }

    @Override
    public UUID getId() {
        return null;
    }
}
