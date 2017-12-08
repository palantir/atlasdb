/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.lock.impl;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;

/**
 * Methods on this class can be called by multiple threads,
 * with synchronization handled by {@link LockServerSync}.
 */
class LockServerLock implements ClientAwareReadWriteLock {
    private static final Logger log = LoggerFactory.getLogger(LockServerLock.class);

    private final LockDescriptor descriptor;
    private final LockClientIndices clients;
    private final LockServerSync sync;

    LockServerLock(LockDescriptor descriptor,
                          LockClientIndices clients) {
        this.descriptor = Preconditions.checkNotNull(descriptor);
        this.clients = clients;
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
        case READ: return new ReadLock(client);
        case WRITE: return new WriteLock(client);
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

    static IllegalMonitorStateException throwIllegalMonitorStateException(String message) {
        IllegalMonitorStateException ex = new IllegalMonitorStateException(message);
        log.error("Illegal monitor state exception: {}", message, ex);
        throw ex;
    }

    private LockClient clientFromIndex(int clientIndex) {
        if (clientIndex == 0) {
            return null;
        }
        return clients.fromIndex(clientIndex);
    }

    private class ReadLock implements KnownClientLock {
        private final int clientIndex;

        private ReadLock(LockClient client) {
            this.clientIndex = clients.toIndex(client);
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
            return clientFromIndex(sync.tryAcquireShared(clientIndex));
        }

        @Override
        public LockClient tryLock(long time, TimeUnit unit) throws InterruptedException {
            return clientFromIndex(sync.tryAcquireSharedNanos(clientIndex, unit.toNanos(time)));
        }

        @Override
        public void changeOwner(LockClient newOwner) {
            sync.changeOwnerShared(clientIndex, clients.toIndex(newOwner));
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
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                    .add("mode", getMode())
                    .add("client", clients.fromIndex(clientIndex))
                    .add("sync", sync)
                    .toString();
        }
    }

    private class WriteLock implements KnownClientLock {
        private final int clientIndex;

        private WriteLock(LockClient client) {
            this.clientIndex = clients.toIndex(client);
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
            return clientFromIndex(sync.tryAcquire(clientIndex));
        }

        @Override
        public LockClient tryLock(long time, TimeUnit unit) throws InterruptedException {
            return clientFromIndex(sync.tryAcquireNanos(clientIndex, unit.toNanos(time)));
        }

        @Override
        public void changeOwner(LockClient newOwner) {
            sync.changeOwner(clientIndex, clients.toIndex(newOwner));
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
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                    .add("mode", getMode())
                    .add("client", clients.fromIndex(clientIndex))
                    .add("sync", sync)
                    .toString();
        }
    }
}
