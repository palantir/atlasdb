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
import com.palantir.logsafe.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.api.map.primitive.MutableIntIntMap;
import org.eclipse.collections.impl.factory.primitive.IntIntMaps;

class LockServerSync extends AbstractQueuedSynchronizer {
    private static final long serialVersionUID = 1L;

    private final LockClientIndices clients;
    private @GuardedBy("this") boolean frozen;
    private @GuardedBy("this") int writeLockHolder = 0;
    private @GuardedBy("this") MutableIntIntMap readLockHolders;

    public LockServerSync(LockClientIndices clients) {
        this.clients = Preconditions.checkNotNull(clients);
    }

    private boolean isAnonymous(int clientIndex) {
        return clientIndex < 0;
    }

    synchronized boolean safeHoldsWriteLock(int clientIndex) {
        return getState() > 0 && clientIndex == writeLockHolder && !isAnonymous(clientIndex);
    }

    private synchronized boolean holdsWriteLock(int clientIndex) {
        Preconditions.checkState(getState() > 0);
        return clientIndex == writeLockHolder && !isAnonymous(clientIndex);
    }

    @Override
    protected synchronized boolean tryAcquire(int clientIndex) {
        if (frozen) {
            return false;
        }
        int writeCount = getState();
        if (writeCount > 0 && holdsWriteLock(clientIndex)) {
            setState(writeCount + 1);
            return true;
        }
        if (hasBlockedPredecessors()) {
            return false;
        }
        if (writeCount == 0 && !isReadLockHeld()) {
            setState(1);
            writeLockHolder = clientIndex;
            return true;
        }
        if (holdsReadLock(clientIndex)) {
            throw LockServerLock.throwIllegalMonitorStateException(
                    clients.fromIndex(clientIndex) + " currently holds the read lock");
        }
        return false;
    }

    @Override
    protected synchronized boolean tryRelease(int clientIndex) {
        int newWriteCount = getState() - 1;
        if (writeLockHolder != clientIndex || newWriteCount < 0) {
            throw LockServerLock.throwIllegalMonitorStateException(
                    clients.fromIndex(clientIndex) + " does not hold the write lock");
        }
        setState(newWriteCount);
        if (newWriteCount == 0 && !isReadLockHeld()) {
            frozen = false;
        }
        return newWriteCount == 0;
    }

    @Override
    protected synchronized int tryAcquireShared(int clientIndex) {
        if (frozen) {
            return -1;
        }
        int writeCount = getState();
        if (writeCount == 0 && !holdsReadLock(clientIndex) && hasBlockedPredecessors()) {
            return -1;
        }
        if (writeCount > 0 && !holdsWriteLock(clientIndex)) {
            return -1;
        }
        incrementReadCount(clientIndex);
        return 1;
    }

    @Override
    protected synchronized boolean tryReleaseShared(int clientIndex) {
        decrementReadCount(clientIndex);
        if (!isReadLockHeld() && getState() == 0) {
            frozen = false;
            return true;
        }
        return false;
    }

    synchronized void unlockAndFreeze(int clientIndex) {
        if (isAnonymous(clientIndex)) {
            throw LockServerLock.throwIllegalMonitorStateException("anonymous clients cannot call unlockAndFreeze()");
        }
        if (writeLockHolder != clientIndex || getState() == 0) {
            throw LockServerLock.throwIllegalMonitorStateException(
                    clients.fromIndex(clientIndex) + " does not hold the write lock");
        }
        if (!release(clientIndex) || isReadLockHeld()) {
            frozen = true;
        }
    }

    synchronized void changeOwnerShared(int oldClient, LockClient newClient) {
        int newIndex = clients.toIndex(newClient);
        if (oldClient == newIndex) {
            throw LockServerLock.throwIllegalMonitorStateException(
                    "new owner must be different  from old owner, owner=" + clients.fromIndex(oldClient));
        }
        if (frozen) {
            throw LockServerLock.throwIllegalMonitorStateException("cannot change owner because the lock is frozen");
        }
        if (!holdsReadLock(oldClient)) {
            throw LockServerLock.throwIllegalMonitorStateException(
                    clients.fromIndex(oldClient) + " does not hold the read lock");
        }
        if (getState() > 0) {
            throw LockServerLock.throwIllegalMonitorStateException(
                    clients.fromIndex(oldClient) + " currently holds both the read and write locks");
        }
        decrementReadCount(oldClient);
        incrementReadCount(newIndex);
    }

    synchronized void changeOwner(int oldClient, LockClient newClient) {
        int newIndex = clients.toIndex(newClient);
        if (oldClient == newIndex) {
            throw LockServerLock.throwIllegalMonitorStateException(
                    "new owner must be different from old owner, owner=" + clients.fromIndex(oldClient));
        }
        if (frozen) {
            throw LockServerLock.throwIllegalMonitorStateException("Cannot change owner because the lock is frozen");
        }
        int writeCount = getState();
        if (writeCount == 0 || writeLockHolder != oldClient) {
            throw LockServerLock.throwIllegalMonitorStateException(
                    clients.fromIndex(oldClient) + " does not hold the write lock");
        }
        if (holdsReadLock(oldClient)) {
            throw LockServerLock.throwIllegalMonitorStateException(
                    clients.fromIndex(oldClient) + " currently holds both the read and write locks");
        }
        if (writeCount > 1) {
            throw LockServerLock.throwIllegalMonitorStateException(
                    clients.fromIndex(oldClient) + " is attempting to create a lock grant"
                            + " while being supported by multiple clients."
                            + " This is not currently supported.");
        }
        writeLockHolder = newIndex;
    }

    // Returns null if there is no lock holder. Note: There
    // may be no lock holder even if tryAcquire or tryAcquireShared
    // failed because those methods may fail in order to prevent
    // barging.
    @GuardedBy("this")
    @Nullable
    LockClient getLockHolder() {
        if (getState() > 0) {
            return clients.fromIndex(writeLockHolder);
        }
        if (!isReadLockHeld()) {
            return null;
        }
        IntIterator iter = readLockHolders.keysView().intIterator();
        Preconditions.checkState(iter.hasNext());
        return clients.fromIndex(iter.next());
    }

    LockClient getClient(int clientIndex) {
        return clients.fromIndex(clientIndex);
    }

    public int getClientIndex(LockClient client) {
        return clients.toIndex(client);
    }

    // See https://bugs.openjdk.java.net/browse/JDK-8191483
    // AbstractQueuedSynchronizer has a bug where simultaneous cancelAcquire() calls can cause
    // future invocations of hasQueuedThreads() and hasQueuedPredecessors() to return true, when in fact there are no
    // queued threads. This causes us to spin indefinitely in LockServerLock#tryLock.
    // To get around this, we use getFirstQueuedThread(), which is not vulnerable to this bug.
    private boolean hasBlockedPredecessors() {
        Thread queuedThread = getFirstQueuedThread();
        return queuedThread != null && queuedThread != Thread.currentThread();
    }

    synchronized boolean isFrozen() {
        return frozen;
    }

    @Override
    public synchronized String toString() {
        return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("hashCode", hashCode())
                .add("writeLockCount", getState())
                .add("writeClient", writeLockHolder == 0 ? null : clients.fromIndex(writeLockHolder))
                .add("readClients", getReadClients())
                .add("queuedThreads", getQueueLength())
                .add("isFrozen", frozen)
                .toString();
    }

    private synchronized boolean isReadLockHeld() {
        return readLockHolders != null && !readLockHolders.isEmpty();
    }

    synchronized boolean holdsReadLock(int clientIndex) {
        return !isAnonymous(clientIndex) && readLockHolders != null && readLockHolders.get(clientIndex) > 0;
    }

    private synchronized void incrementReadCount(int clientIndex) {
        if (readLockHolders == null) {
            readLockHolders = IntIntMaps.mutable.ofInitialCapacity(1);
        }
        readLockHolders.addToValue(clientIndex, 1);
    }

    private synchronized void decrementReadCount(int clientIndex) {
        if (readLockHolders == null) {
            throw LockServerLock.throwIllegalMonitorStateException(
                    clients.fromIndex(clientIndex) + " does not hold the read lock");
        }
        int readCount = readLockHolders.removeKeyIfAbsent(clientIndex, 0);
        if (readCount > 1) {
            readLockHolders.put(clientIndex, readCount - 1);
        } else if (readCount == 0) {
            throw LockServerLock.throwIllegalMonitorStateException(
                    clients.fromIndex(clientIndex) + " does not hold the read lock");
        }
    }

    synchronized List<LockClient> getReadClients() {
        if (readLockHolders == null) {
            return List.of();
        }
        return Collections.unmodifiableList(
                readLockHolders.keysView().collect(clients::fromIndex, new ArrayList<>(readLockHolders.size())));
    }

    synchronized Optional<LockClient> getWriteClient() {
        if (getState() <= 0) {
            return Optional.empty();
        }
        return Optional.of(clients.fromIndex(writeLockHolder));
    }
}
