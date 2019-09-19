/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.lock.impl;

import com.google.common.base.MoreObjects;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.logsafe.Preconditions;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockServerLock implements ClientAwareReadWriteLock {
    private static final Logger log = LoggerFactory.getLogger(LockServerLock.class);

    private final LockDescriptor descriptor;
    private final LockServerSync sync;

    public LockServerLock(LockDescriptor descriptor,
                          LockClientIndices clients) {
        this.descriptor = Preconditions.checkNotNull(descriptor);
        this.sync = new LockServerSync(clients);
    }

    @Override
    public LockDescriptor getDescriptor() {
        return descriptor;
    }

    @Override
    public KnownClientLock get(LockClient client, LockMode mode) {
        Preconditions.checkNotNull(client);
        switch (mode) {
        case READ: return new ReadLock(sync, client);
        case WRITE: return new WriteLock(sync, client);
        default: throw new EnumConstantNotPresentException(LockMode.class, mode.name());
        }
    }

    @Override
    public boolean isFrozen() {
        return sync.isFrozen();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("descriptor", descriptor)
                .add("sync", sync)
                .toString();
    }

    public String toSanitizedString() {
        return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("sync", sync)
                .toString();
    }

    static IllegalMonitorStateException throwIllegalMonitorStateException(String message) {
        IllegalMonitorStateException ex = new IllegalMonitorStateException(message);
        log.error("Illegal monitor state exception: {}", message, ex);
        throw ex;
    }

    private static class ReadLock implements KnownClientLock {
        private final LockServerSync sync;
        private final int clientIndex;

        public ReadLock(LockServerSync sync, LockClient client) {
            this.sync = sync;
            this.clientIndex = sync.getClientIndex(client);
        }

        @Override
        public LockMode getMode() {
            return LockMode.READ;
        }

        @Override
        public void lock() {
            sync.acquireShared(clientIndex);
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            sync.acquireSharedInterruptibly(clientIndex);
        }

        @Override
        public LockClient tryLock() {
            while (true) {
                synchronized (sync) {
                    if (sync.tryAcquireShared(clientIndex) > 0) {
                        return null;
                    }
                    LockClient lockHolder = sync.getLockHolder();
                    if (lockHolder != null) {
                        return lockHolder;
                    }
                }
                // the lock was free, but we couldn't acquire it because
                // someone else was waiting for it (no barging). Yield
                // to give them a chance to grab it.
                Thread.yield();
            }
        }

        @Override
        public LockClient tryLock(long time, TimeUnit unit) throws InterruptedException {
            LockClient client = tryLock();
            if (client != null && sync.tryAcquireSharedNanos(clientIndex, unit.toNanos(time))) {
                return null;
            }
            return client;
        }

        @Override
        public void changeOwner(LockClient newOwner) {
            sync.changeOwnerShared(clientIndex, newOwner);
        }

        @Override
        public void unlock() {
            sync.releaseShared(clientIndex);
        }

        @Override
        public void unlockAndFreeze() {
            throw throwIllegalMonitorStateException("read locks cannot be frozen");
        }

        @Override
        public boolean isHeld() {
            return sync.holdsReadLock(clientIndex);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                    .add("mode", getMode())
                    .add("client", sync.getClient(clientIndex))
                    .add("sync", sync)
                    .toString();
        }
    }

    private static class WriteLock implements KnownClientLock {
        private final LockServerSync sync;
        private final int clientIndex;

        public WriteLock(LockServerSync sync, LockClient client) {
            this.sync = sync;
            this.clientIndex = sync.getClientIndex(client);
        }

        @Override
        public LockMode getMode() {
            return LockMode.WRITE;
        }

        @Override
        public void lock() {
            sync.acquire(clientIndex);
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            sync.acquireInterruptibly(clientIndex);
        }

        @Override
        public LockClient tryLock() {
            while (true) {
                synchronized (sync) {
                    if (sync.tryAcquire(clientIndex)) {
                        return null;
                    }
                    LockClient lockHolder = sync.getLockHolder();
                    if (lockHolder != null) {
                        return lockHolder;
                    }
                }
                // the lock was free, but we couldn't acquire it because
                // someone else was waiting for it (no barging). Yield
                // to give them a chance to grab it.
                Thread.yield();
            }
        }

        @Override
        public LockClient tryLock(long time, TimeUnit unit) throws InterruptedException {
            LockClient client = tryLock();
            if (client != null && sync.tryAcquireNanos(clientIndex, unit.toNanos(time))) {
                return null;
            }
            return client;
        }

        @Override
        public void changeOwner(LockClient newOwner) {
            sync.changeOwner(clientIndex, newOwner);
        }

        @Override
        public void unlock() {
            sync.release(clientIndex);
        }

        @Override
        public void unlockAndFreeze() {
            sync.unlockAndFreeze(clientIndex);
        }

        @Override
        public boolean isHeld() {
            return sync.safeHoldsWriteLock(clientIndex);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                    .add("mode", getMode())
                    .add("client", sync.getClient(clientIndex))
                    .add("sync", sync)
                    .toString();
        }
    }
}
