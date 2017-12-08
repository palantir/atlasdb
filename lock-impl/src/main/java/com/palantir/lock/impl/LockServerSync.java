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

import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.primitives.Ints;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;

/**
 * Main synchronization logic for LockServer read-write locks.
 *
 * LockServer read-write locks have the following semantics in addition to standard read-write ones:
 * <ul>
 *     <li>Non-anonymous lock client lock requests are reentrant.</li>
 *     <li>A non-anonymous lock client which holds a write lock can also reentrantly be granted a read lock.</li>
 *     <li>Requests are fair (no-barging) and ordered by when the client's first request was received.</li>
 *     <li>Write locks can be <i>frozen</i> by a client, which disables additional reentrant lock requests.</li>
 *     <li>A lock client can transfer its lock hold to another client, unless doing so would violate read-write client invariants.</li>
 * </ul>
 *
 * Thread safety is maintained by <pre>synchronized</pre> methods --
 * state cannot be modified without first grabbing the object lock.
 *
 * This class is based originally on {@link java.util.concurrent.locks.AbstractQueuedSynchronizer},
 * but no longer extends it due to JDK-8191483.
 */
class LockServerSync {

    // All our state is managed in indices, but we use this for diagnostics
    // (exception messages, toString())
    private final LockClientIndices clients;

    LockServerSync(LockClientIndices clients) {
        this.clients = clients;
    }

    private boolean frozen = false;
    private int writeLockHolder = 0;
    private int writeLockCount = 0;

    // Queue of clients waiting for the lock.
    // clients which are not anonymous will also have an entry in waitingReads/waitingWrites
    private int queueLength = 0;
    private Node head;
    private Node tail;

    // The following fields MUST NOT be accessed directly!
    // Access must be through the utility methods located at the bottom of this class,
    // Which are prefixed by the field name. (e.g. readLockHoldersContainsKey()
    private TIntIntMap readLockHolders = null; // client index -> re-entrance count
    private TIntObjectMap<Node> waitingReads = null;
    private TIntObjectMap<Node> waitingWrites = null;

    /**
     * Doubly-linked-list node,
     * representing a given lock client's request for the lock.
     *
     * Requesters will await() on this node until it is possible to grab the lock,
     * at which point they will be notify()'d when they have reached the front of the line
     * and can re-contend for the lock.
     *
     * You must hold the owning LockServerSync object lock before accessing or calling
     * methods on a Node object.
     */
    private static final class Node {
        public Node prev = null;
        public Node next = null;

        /**
         * Number of reentrant requests from the client. Should always be 1 for anonymous clients.
         * There will be exactly <pre>count</pre> threads await()'ing on this node.
         */
        public int count = 0;

        private long signalCount = 0;

        public synchronized void signal() {
            ++signalCount;
            notifyAll();
        }

        public synchronized long prepAwait() {
            return signalCount;
        }

        public synchronized void await(long oldSignalCount, long nanos) throws InterruptedException {
            long t0 = System.nanoTime();
            while (true) {
                if (signalCount != oldSignalCount) {
                    return;
                }
                long left = nanos - Math.max(0, System.nanoTime() - t0);
                if (left <= 0) {
                    return;
                }
                wait(left / 1000000, (int) (left % 1000000));
            }
        }
    }

    private boolean tryAcquire(@Nullable Node node, int clientIndex, boolean shared) {
        assert Thread.holdsLock(this);

        if (frozen) {
            return false;
        }

        if (shared) {
            if (clientIndex != -1 && readLockHoldersContainsKey(clientIndex)) {
                // reentrant read lock
                readLockHoldersInc(clientIndex);
                return true;
            }

            if (clientIndex != -1 && writeLockHolder == clientIndex) {
                // reentrant write -> read lock
                readLockHoldersInc(clientIndex);
                return true;
            }

            if (writeLockCount > 0) {
                return false;
            }

            if (head != null && head != node) {
                return false;
            }

            readLockHoldersInc(clientIndex);
            return true;
        } else {
            if (clientIndex != -1 && writeLockHolder == clientIndex) {
                // reentrant write lock
                ++writeLockCount;
                return true;
            }

            if (clientIndex != -1 && readLockHoldersContainsKey(clientIndex)) {
                // reentrant read -> write, conspicuously we fail this
                throw LockServerLock.throwIllegalMonitorStateException(clients.fromIndex(clientIndex) + " currently holds the read lock");
            }

            if (!readLockHoldersIsEmpty()) {
                return false;
            }

            if (head != null && head != node) {
                return false;
            }

            if (writeLockCount > 0) {
                return false;
            }

            writeLockHolder = clientIndex;
            writeLockCount = 1;
            return true;
        }
    }

    private int getCurrentHolder() {
        assert Thread.holdsLock(this);

        if (writeLockCount > 0) {
            return writeLockHolder;
        }
        return readLockHoldersFirstKeyOrZero();
    }

    private void notifyNode(@Nullable Node node) {
        assert Thread.holdsLock(this);

        if (node == null) {
            return;
        }
        node.signal();
    }

    private Node refNode(int clientIndex, boolean shared) {
        assert Thread.holdsLock(this);

        Node node;
        if (clientIndex == -1) {
            node = null;
        } else if (shared) {
            node = waitingReadsGet(clientIndex);
        } else {
            node = waitingWritesGet(clientIndex);
        }
        if (node == null) {
            node = new Node();
            if (clientIndex == -1) {
                // no coalescion
            } else if (shared) {
                waitingReadsPut(clientIndex, node);
            } else {
                waitingWritesPut(clientIndex, node);
            }
            if (tail == null) {
                head = node;
            } else {
                tail.next = node;
            }
            node.prev = tail;
            tail = node;
            ++queueLength;
        }
        ++node.count;
        return node;
    }

    /**
     * Leave the line of waiting nodes, and if by doing so you change who's first in line,
     * notify() them.
     */
    private void unrefNode(int clientIndex, boolean shared, Node node) {
        assert Thread.holdsLock(this);

        if (--node.count > 0) {
            return;
        }

        if (clientIndex == -1) {
            // not in either waiting map
        } else if (shared) {
            waitingReadsRemove(clientIndex);
        } else {
            waitingWritesRemove(clientIndex);
        }
        if (node.prev == null) {
            head = node.next;
            notifyNode(head);
        } else {
            node.prev.next = node.next;
        }

        if (node.next == null) {
            tail = node.prev;
        } else {
            node.next.prev = node.prev;
        }

        --queueLength;
    }

    /**
     * @return returns 0 if the lock was acquired successfully, otherwise the client index of one of the current holders.
     */
    private int acquireCommon(int clientIndex, boolean shared, long nanos, boolean allowInterrupt) throws InterruptedException {
        long t0 = System.nanoTime();

        // Try to hit/miss quickly (without enqueueing)...
        Node node;
        synchronized (this) {
            if (tryAcquire(null, clientIndex, shared)) {
                // We got our lock, great.
                return 0;
            }

            // We didn't get our lock.  Are we supposed to wait?
            if (nanos == 0) {
                // If not, see if we can figure out who holds it.
                int blocker = getCurrentHolder();
                if (blocker != 0) {
                    // We have a holder, we're good to go.
                    return blocker;
                }

                // Couldn't take it, but no one holds?  Presumably someone is in
                // line and they need to process first.  We go the long way and
                // enqueue, even though nanos == 0 was given.
            } else {
                // Fall through to enqueue...
            }

            // Get in line or join a node already in line.
            node = refNode(clientIndex, shared);
        }

        boolean wasInterrupted = false;
        try {
            while (true) {
                // Either acquire, fail to acquire, or figure out the
                // parameters of our wait...
                long signalCount;
                long left;
                synchronized (this) {
                    if (tryAcquire(node, clientIndex, shared)) {
                        // Successful acquire, return.
                        return 0;
                    }

                    left = nanos - Math.max(0, System.nanoTime() - t0);

                    if (left <= 0) {
                        // Out of time!  Try to figure out who has it.
                        int blocker = getCurrentHolder();
                        if (blocker != 0) {
                            return blocker;
                        }

                        // Again, we couldn't take it, but no one holds?  Spin and
                        // wait for the line to process.
                        left = Long.MAX_VALUE;
                    }

                    // We want to node.wait() here but don't want to hold lock
                    // on sync itself during.  We note the current signalCount
                    // so if a signal happens between here and below where we
                    // would wait we'll actually know (and not wait()).
                    signalCount = node.prepAwait();
                }

                try {
                    // Wait up to however long for an update.
                    node.await(signalCount, left);
                } catch (InterruptedException e) {
                    if (allowInterrupt) {
                        throw e;
                    } else {
                        wasInterrupted = true;
                    }
                }
            }
        } finally {
            synchronized (this) {
                // Leave line on the way out no matter why/how we're returning.
                unrefNode(clientIndex, shared, node);
            }
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private int acquireCommon(int clientIndex, boolean shared, long nanos) {
        try {
            return acquireCommon(clientIndex, shared, nanos, false);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private int acquireCommonInterruptibly(int clientIndex, boolean shared, long nanos) throws InterruptedException {
        return acquireCommon(clientIndex, shared, nanos, true);
    }

    public void acquire(int clientIndex) {
        acquireCommon(clientIndex, false, Long.MAX_VALUE);
    }

    public void acquireInterruptibly(int clientIndex) throws InterruptedException {
        acquireCommonInterruptibly(clientIndex, false, Long.MAX_VALUE);
    }

    public void acquireShared(int clientIndex) {
        acquireCommon(clientIndex, true, Long.MAX_VALUE);
    }

    public void acquireSharedInterruptibly(int clientIndex) throws InterruptedException {
        acquireCommonInterruptibly(clientIndex, true, Long.MAX_VALUE);
    }

    public int tryAcquire(int clientIndex) {
        return acquireCommon(clientIndex, false, 0);
    }

    public int tryAcquireNanos(int clientIndex, long nanos) throws InterruptedException {
        return acquireCommonInterruptibly(clientIndex, false, nanos);
    }

    public int tryAcquireShared(int clientIndex) {
        return acquireCommon(clientIndex, true, 0);
    }

    public int tryAcquireSharedNanos(int clientIndex, long nanos) throws InterruptedException {
        return acquireCommonInterruptibly(clientIndex, true, nanos);
    }

    public synchronized void release(int clientIndex) {
        if (writeLockHolder != clientIndex) {
            throw LockServerLock.throwIllegalMonitorStateException(clients.fromIndex(clientIndex) + " does not hold the write lock");
        }
        if (--writeLockCount == 0) {
            writeLockHolder = 0;
            frozen = false;
            notifyNode(head);
        }
    }

    public synchronized void releaseShared(int clientIndex) {
        if (!readLockHoldersContainsKey(clientIndex)) {
            throw LockServerLock.throwIllegalMonitorStateException(clients.fromIndex(clientIndex) + " does not hold a read lock");
        }
        if (readLockHoldersDec(clientIndex) == 0) {
            frozen = false;
            notifyNode(head);
        }
    }

    public synchronized void unlockAndFreeze(int clientIndex) {
        if (writeLockHolder != clientIndex) {
            throw LockServerLock.throwIllegalMonitorStateException(clients.fromIndex(clientIndex) + " does not hold the write lock");
        }
        if (clientIndex == -1) {
            throw LockServerLock.throwIllegalMonitorStateException("anonymous clients cannot call unlockAndFreeze()");
        }
        if (--writeLockCount == 0) {
            writeLockHolder = 0;
            frozen = false;
            notifyNode(head);
        } else {
            frozen = true;
        }
    }

    public synchronized void changeOwner(int oldIndex, int newIndex) {
        if (frozen) {
            throw LockServerLock.throwIllegalMonitorStateException("cannot change owner because the lock is frozen");
        }
        if (writeLockHolder != oldIndex) {
            throw LockServerLock.throwIllegalMonitorStateException(clients.fromIndex(oldIndex) + " does not hold a write lock");
        }
        if (!readLockHoldersIsEmpty()) {
            throw LockServerLock.throwIllegalMonitorStateException(clients.fromIndex(oldIndex) + " currently holds both the read and write locks");
        }
        if (writeLockCount > 1) {
            throw LockServerLock.throwIllegalMonitorStateException(clients.fromIndex(oldIndex) + " is attempting to create a lock grant while being supported by multiple clients.  This is not currently supported.");
        }

        writeLockHolder = newIndex;
        // may have to wake up waiters that are now reentrant
        notifyNode(waitingReadsGet(newIndex));
        notifyNode(waitingWritesGet(newIndex));
    }

    public synchronized void changeOwnerShared(int oldIndex, int newIndex) {
        if (frozen) {
            throw LockServerLock.throwIllegalMonitorStateException("cannot change owner because the lock is frozen");
        }
        if (!readLockHoldersContainsKey(oldIndex)) {
            throw LockServerLock.throwIllegalMonitorStateException(clients.fromIndex(oldIndex) + " does not hold a read lock");
        }
        if (writeLockCount > 0) {
            throw LockServerLock.throwIllegalMonitorStateException(clients.fromIndex(oldIndex) + " currently holds both a read and a write locks");
        }

        readLockHoldersDec(oldIndex);

        if (readLockHoldersInc(newIndex) == 1) {
            // first lock for new client, may have to wake up waiters that are
            // now reentrant reads
            notifyNode(waitingReadsGet(newIndex));
        }
    }

    public synchronized boolean isFrozen() {
        return frozen;
    }

    @Override
    public synchronized String toString() {
        return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("hashCode", hashCode())
                .add("writeLockCount", writeLockCount)
                .add("writeClient", writeLockHolder == 0 ? null : clients.fromIndex(writeLockHolder))
                .add("readClients", clients.fromIndices(Ints.asList(readLockHoldersKeys())))
                .add("queuedThreads", queueLength)
                .add("isFrozen", frozen)
                .toString();
    }


    // methods for using readLockHolders

    private boolean readLockHoldersContainsKey(int clientIndex) {
        return readLockHolders != null && readLockHolders.containsKey(clientIndex);
    }

    private int[] readLockHoldersKeys() {
        return readLockHolders == null ? new int[0] : readLockHolders.keys();
    }

    private boolean readLockHoldersIsEmpty() {
        return readLockHolders == null;
    }

    private int readLockHoldersFirstKeyOrZero() {
        if (readLockHolders == null) {
            return 0;
        }
        TIntIntIterator i = readLockHolders.iterator();
        i.advance();
        return i.key();
    }

    private int readLockHoldersInc(int clientIndex) {
        if (readLockHolders == null) {
            readLockHolders = new TIntIntHashMap(1);
        }
        return readLockHolders.adjustOrPutValue(clientIndex, 1, 1);
    }

    private int readLockHoldersDec(int clientIndex) {
        assert readLockHolders != null;
        int ret = readLockHolders.adjustOrPutValue(clientIndex, -1, -1);
        assert ret >= 0;
        if (ret == 0) {
            readLockHolders.remove(clientIndex);
        }
        if (readLockHolders.isEmpty()) {
            readLockHolders = null;
        }
        return ret;
    }


    // methods for using waitingReads

    private Node waitingReadsGet(int clientIndex) {
        if (waitingReads == null) {
            return null;
        }
        return waitingReads.get(clientIndex);
    }

    private void waitingReadsPut(int clientIndex, Node node) {
        if (waitingReads == null) {
            waitingReads = new TIntObjectHashMap<>(1);
        }
        waitingReads.put(clientIndex, node);
    }

    private void waitingReadsRemove(int clientIndex) {
        assert waitingReads != null;
        waitingReads.remove(clientIndex);
        if (waitingReads.isEmpty()) {
            waitingReads = null;
        }
    }


    // methods for using waitingWrites

    private Node waitingWritesGet(int clientIndex) {
        if (waitingWrites == null) {
            return null;
        }
        return waitingWrites.get(clientIndex);
    }

    private void waitingWritesPut(int clientIndex, Node node) {
        if (waitingWrites == null) {
            waitingWrites = new TIntObjectHashMap<>(1);
        }
        waitingWrites.put(clientIndex, node);
    }

    private void waitingWritesRemove(int clientIndex) {
        assert waitingWrites != null;
        waitingWrites.remove(clientIndex);
        if (waitingWrites.isEmpty()) {
            waitingWrites = null;
        }
    }
}
