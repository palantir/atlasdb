/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.persistentlock;

import com.google.common.collect.ForwardingObject;

public final class InitialisingPersistentLockService extends ForwardingObject implements PersistentLockService {
    private volatile PersistentLockService delegate;

    private InitialisingPersistentLockService(PersistentLockService persistentLockService) {
        delegate = persistentLockService;
    }

    public static InitialisingPersistentLockService create() {
        return new InitialisingPersistentLockService(null);
    }

    public void initialise(PersistentLockService persistentLockService) {
        delegate = persistentLockService;
    }

    @Override
    public PersistentLockId acquireBackupLock(String reason) {
        return getDelegate().acquireBackupLock(reason);
    }

    @Override
    public void releaseBackupLock(PersistentLockId lockId) {
        getDelegate().releaseBackupLock(lockId);
    }

    private PersistentLockService getDelegate() {
        return (PersistentLockService) delegate();
    }

    @Override
    protected Object delegate() {
        checkInitialised();
        return delegate;
    }

    void checkInitialised() {
        if (delegate == null) {
            throw new IllegalStateException("Not initialised");
        }
    }
}
