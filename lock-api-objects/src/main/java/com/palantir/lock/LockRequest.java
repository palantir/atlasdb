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
package com.palantir.lock;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.palantir.logsafe.Preconditions;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * An encapsulation of all parameters needed to make a locking request to the
 * lock server.
 *
 * @author jtamer
 */
@JsonDeserialize(builder = LockRequest.SerializationProxy.class)
@Immutable
public final class LockRequest implements Serializable {
    private static final long serialVersionUID = 0xf6c12b970b44af68L;

    private static final AtomicReference<TimeDuration> DEFAULT_LOCK_TIMEOUT =
            new AtomicReference<>(SimpleTimeDuration.of(120, TimeUnit.SECONDS));

    /** The default amount of time that it takes a lock (lease) to expire. */
    public static TimeDuration getDefaultLockTimeout() {
        return DEFAULT_LOCK_TIMEOUT.get();
    }

    /**
     * Sets the default lock timeout for all lock requests that do not specify an explicit timeout. See {@link
     * Builder#timeoutAfter}.
     */
    public static void setDefaultLockTimeout(TimeDuration timeout) {
        Preconditions.checkNotNull(timeout, "timeout cannot be null");
        Preconditions.checkArgument(timeout.getTime() > 0, "timeout must be > 0");

        DEFAULT_LOCK_TIMEOUT.set(timeout);
    }

    private static volatile String localServerName = "";

    private final SortedLockCollection<LockDescriptor> lockMap;
    private final TimeDuration lockTimeout;
    private final LockGroupBehavior lockGroupBehavior;
    private final BlockingMode blockingMode;

    @Nullable
    private final TimeDuration blockingDuration;

    @Nullable
    private final Long versionId;

    private transient int hashCode;

    private final String creatingThreadName;

    /** Creates a new lock request builder for the given locks. */
    public static Builder builder(SortedMap<LockDescriptor, LockMode> lockMap) {
        return new Builder(lockMap);
    }

    /** Creates a new lock request builder for the given locks. */
    public static Builder builder(SortedLockCollection<LockDescriptor> locks) {
        return new Builder(locks);
    }

    private LockRequest(
            SortedLockCollection<LockDescriptor> lockMap,
            TimeDuration lockTimeout,
            LockGroupBehavior lockGroupBehavior,
            BlockingMode blockingMode,
            @Nullable TimeDuration blockingDuration,
            @Nullable Long versionId,
            String creatingThreadName) {
        this.lockMap = lockMap;
        this.lockTimeout = lockTimeout;
        this.lockGroupBehavior = lockGroupBehavior;
        this.blockingMode = blockingMode;
        this.blockingDuration = blockingDuration;
        this.versionId = versionId;
        this.creatingThreadName = creatingThreadName;
    }

    /**
     * Returns the mapping from descriptor to lock mode for this request. Note
     * that some of the locks in this map might not have been acquired if the
     * lock group behavior is set to
     * {@link LockGroupBehavior#LOCK_AS_MANY_AS_POSSIBLE}. To get the set of
     * locks which were actually acquired successfully, use
     * {@link HeldLocksToken#getLockDescriptors()} instead.
     */
    @JsonIgnore
    public SortedLockCollection<LockDescriptor> getLockDescriptors() {
        return lockMap;
    }

    public List<LockWithMode> getLocks() {
        return ImmutableList.copyOf(Iterables.transform(lockMap.entries(), LockRequest::toLockWithModeFunction));
    }

    /**
     * Returns the amount of time that it takes for these locks (leases) to
     * expire.
     */
    public TimeDuration getLockTimeout() {
        return lockTimeout;
    }

    /**
     * Returns the algorithm used by the lock server when some of the requested
     * locks cannot be acquired.
     */
    public LockGroupBehavior getLockGroupBehavior() {
        return lockGroupBehavior;
    }

    /**
     * Returns the blocking mode for this request, which determines whether the
     * lock server will block indefinitely, block for a fixed amount of time, or
     * not block.
     */
    public BlockingMode getBlockingMode() {
        return blockingMode;
    }

    /**
     * Returns the maximum amount of time that the lock server will block while
     * acquiring locks, or <code>null</code> if the blocking mode is not set to
     * {@link BlockingMode#BLOCK_UNTIL_TIMEOUT}.
     */
    @Nullable
    public TimeDuration getBlockingDuration() {
        return blockingDuration;
    }

    /**
     * Returns the version ID for this request, or <code>null</code> if this request
     * did not specify a version ID.
     *
     * @see LockService#getMinLockedInVersionId()
     */
    @Nullable
    public Long getVersionId() {
        return versionId;
    }

    /**
     * Returns the name of the thread which created this LockRequest. This is useful
     * for debugging
     */
    public String getCreatingThreadName() {
        return creatingThreadName;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LockRequest)) {
            return false;
        }
        LockRequest other = (LockRequest) obj;
        return lockMap.equals(other.lockMap)
                && lockTimeout.equals(other.lockTimeout)
                && (lockGroupBehavior == other.lockGroupBehavior)
                && (blockingMode == other.blockingMode)
                && Objects.equals(blockingDuration, other.blockingDuration)
                && Objects.equals(versionId, other.versionId);
    }

    @Override
    public int hashCode() {
        int tempHashCode = hashCode;
        if (tempHashCode == 0) {
            tempHashCode =
                    Objects.hash(lockMap, lockTimeout, lockGroupBehavior, blockingMode, blockingDuration, versionId);
            hashCode = tempHashCode;
        }
        return tempHashCode;
    }

    public static void setLocalServerName(String serverName) {
        if (serverName == null || serverName.trim().isEmpty()) {
            localServerName = "";
            return;
        }
        localServerName = serverName;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass().getSimpleName())
                .omitNullValues()
                .add("lockCount", lockMap.size())
                .add("firstLock", lockMap.entries().iterator().next())
                .add("lockTimeout", lockTimeout)
                .add("lockGroupBehavior", lockGroupBehavior)
                .add("blockingMode", blockingMode)
                .add("blockingDuration", blockingDuration)
                .add("versionId", versionId)
                .toString();
    }

    private void readObject(@SuppressWarnings("unused") ObjectInputStream in) throws InvalidObjectException {
        throw new InvalidObjectException("proxy required");
    }

    private Object writeReplace() {
        return new SerializationProxy(this);
    }

    private static LockWithMode toLockWithModeFunction(Map.Entry<LockDescriptor, LockMode> input) {
        return new LockWithMode(input.getKey(), input.getValue());
    }
    /**
     * A helper class used to construct an immutable {@link LockRequest}
     * instance.
     *
     * @author jtamer
     */
    @NotThreadSafe
    public static final class Builder {
        @Nullable
        private SortedLockCollection<LockDescriptor> lockMap;

        @Nullable
        private TimeDuration lockTimeout;

        @Nullable
        private LockGroupBehavior lockGroupBehavior;

        @Nullable
        private BlockingMode blockingMode;

        @Nullable
        private TimeDuration blockingDuration;

        @Nullable
        private Long versionId;

        @Nullable
        private String creatingThreadName;

        private Builder(SortedMap<LockDescriptor, LockMode> lockMap) {
            this(LockCollections.of(lockMap));
        }

        private Builder(SortedLockCollection<LockDescriptor> lockMap) {
            this.lockMap = lockMap;
            Preconditions.checkArgument(!this.lockMap.isEmpty());
        }

        /**
         * Instructs the lock server to release these locks if a refresh request
         * has not been received for the period of time represented by the
         * <code>lockTimeout</code> parameter. The default value is controlled
         * by {@link LockRequest#getDefaultLockTimeout()}.
         * You may not call this method multiple times.
         */
        public Builder timeoutAfter(TimeDuration newLockTimeout) {
            Preconditions.checkArgument(newLockTimeout.toMillis() > 0);
            if ((lockMap == null) || (this.lockTimeout != null)) {
                throw new IllegalStateException();
            }
            this.lockTimeout = SimpleTimeDuration.of(newLockTimeout);
            return this;
        }

        /**
         * Instructs the lock server to block for at most the given duration
         * when acquiring locks. By default, the lock server will block
         * indefinitely. You may not call this method multiple times, and you
         * may not call both {@link #doNotBlock()} and this method.
         */
        public Builder blockForAtMost(TimeDuration newBlockingDuration) {
            Preconditions.checkNotNull(newBlockingDuration, "newBlockingDuration should not be null");
            TimeDuration realBlockingDuration = SimpleTimeDuration.of(newBlockingDuration);
            Preconditions.checkArgument(realBlockingDuration.toNanos() >= 0);
            if (realBlockingDuration.toNanos() == 0) {
                return doNotBlock();
            }
            if ((lockMap == null) || (blockingMode != null)) {
                throw new IllegalStateException();
            }
            blockingMode = BlockingMode.BLOCK_UNTIL_TIMEOUT;
            this.blockingDuration = realBlockingDuration;
            return this;
        }

        /**
         * Instructs the lock server to not block at all when acquiring locks.
         * By default, the lock server will block indefinitely. You may not
         * call this method multiple times, and you may not call both
         * {@code blockForAtMost()} and this method.
         */
        public Builder doNotBlock() {
            if ((lockMap == null) || (blockingMode != null)) {
                throw new IllegalStateException();
            }
            blockingMode = BlockingMode.DO_NOT_BLOCK;
            return this;
        }

        /**
         * Instructs the lock server to acquire as many of the requested locks
         * as possible. By default, if any of the requested locks cannot be
         * acquired, then none of them will be acquired. You may not call this
         * method multiple times, and if you call this method, then you must
         * also call either {@link #doNotBlock()} or one of the
         * {@code blockForAtMost()} methods before calling {@link #build()}.
         */
        public Builder lockAsManyAsPossible() {
            if ((lockMap == null) || (lockGroupBehavior != null)) {
                throw new IllegalStateException();
            }
            Preconditions.checkState(blockingMode != BlockingMode.BLOCK_INDEFINITELY_THEN_RELEASE);
            lockGroupBehavior = LockGroupBehavior.LOCK_AS_MANY_AS_POSSIBLE;
            return this;
        }

        /**
         * The lock server will lock as usual, but it will immediately release the locks as they
         * are acquired.  This can be used to avoid round trips if you are just going to call unlock
         * as soon as you call lock.
         * <p>
         * It is NOT guaranteed that all locks will be held at the same time.  The lock server may
         * lock and release each one separately.
         * <p>
         * This is commonly used if you need to wait for someone to release a lock before you may
         * proceed.
         * <p>
         * NOTE: this is incompatible with {@link #lockAsManyAsPossible()} and {@link #withLockedInVersionId(long)}
         */
        public Builder lockAndRelease() {
            if ((lockMap == null) || (blockingMode != null)) {
                throw new IllegalStateException();
            }
            Preconditions.checkState(lockGroupBehavior != LockGroupBehavior.LOCK_AS_MANY_AS_POSSIBLE);
            blockingMode = BlockingMode.BLOCK_INDEFINITELY_THEN_RELEASE;
            return this;
        }

        /**
         * Associates the given locked-in version ID with this request. You may
         * not call this method multiple times.
         *
         * @see LockService#getMinLockedInVersionId()
         */
        public Builder withLockedInVersionId(long newVersionId) {
            if ((lockMap == null) || (this.versionId != null)) {
                throw new IllegalStateException();
            }
            this.versionId = newVersionId;
            return this;
        }

        public Builder withCreatingThreadName(String threadName) {
            this.creatingThreadName = threadName;
            return this;
        }

        /**
         * Builds a {@link LockRequest} instance. After calling this method, the
         * builder object is invalidated.
         */
        public LockRequest build() {
            if ((lockMap == null) || ((blockingMode == null) && (lockGroupBehavior != null))) {
                throw new IllegalStateException();
            }
            if (versionId != null && blockingMode == BlockingMode.BLOCK_INDEFINITELY_THEN_RELEASE) {
                throw new IllegalStateException();
            }
            String serverName = "";
            if (!localServerName.isEmpty()) {
                serverName = " (on server " + localServerName + ")";
            }
            LockRequest request = new LockRequest(
                    lockMap,
                    MoreObjects.firstNonNull(lockTimeout, getDefaultLockTimeout()),
                    MoreObjects.firstNonNull(lockGroupBehavior, LockGroupBehavior.LOCK_ALL_OR_NONE),
                    MoreObjects.firstNonNull(blockingMode, BlockingMode.BLOCK_INDEFINITELY),
                    blockingDuration,
                    versionId,
                    MoreObjects.firstNonNull(
                                    creatingThreadName, Thread.currentThread().getName())
                            + serverName);
            lockMap = null;
            return request;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass().getSimpleName())
                    .add("lockTimeout", lockTimeout)
                    .add("lockGroupBehavior", lockGroupBehavior)
                    .add("blockingMode", blockingMode)
                    .add("blockingDuration", blockingDuration)
                    .add("versionId", versionId)
                    .add("lockMap", lockMap)
                    .toString();
        }
    }

    static class SerializationProxy implements Serializable {
        private static final long serialVersionUID = 0xd6b8378030ed100dL;

        private final SortedLockCollection<LockDescriptor> lockMap;
        private final TimeDuration lockTimeout;
        private final LockGroupBehavior lockGroupBehavior;
        private final BlockingMode blockingMode;

        @Nullable
        private final TimeDuration blockingDuration;

        @Nullable
        private final Long versionId;

        private final String creatingThreadName;

        SerializationProxy(LockRequest lockRequest) {
            lockMap = lockRequest.lockMap;
            lockTimeout = lockRequest.lockTimeout;
            lockGroupBehavior = lockRequest.lockGroupBehavior;
            blockingMode = lockRequest.blockingMode;
            blockingDuration = lockRequest.blockingDuration;
            versionId = lockRequest.versionId;
            creatingThreadName = lockRequest.creatingThreadName;
        }

        @JsonCreator
        SerializationProxy(
                @JsonProperty("locks") List<LockWithMode> locks,
                @JsonProperty("lockTimeout") TimeDuration lockTimeout,
                @JsonProperty("lockGroupBehavior") LockGroupBehavior lockGroupBehavior,
                @JsonProperty("blockingMode") BlockingMode blockingMode,
                @JsonProperty("blockingDuration") TimeDuration blockingDuration,
                @JsonProperty("versionId") Long versionId,
                @JsonProperty("creatingThreadName") String creatingThreadName) {
            ImmutableSortedMap.Builder<LockDescriptor, LockMode> localLockMapBuilder =
                    ImmutableSortedMap.naturalOrder();
            for (LockWithMode lock : locks) {
                localLockMapBuilder.put(lock.getLockDescriptor(), lock.getLockMode());
            }
            this.lockMap = LockCollections.of(localLockMapBuilder.build());
            this.lockTimeout = lockTimeout;
            this.lockGroupBehavior = lockGroupBehavior;
            this.blockingMode = blockingMode;
            this.blockingDuration = blockingDuration;
            this.versionId = versionId;
            this.creatingThreadName = creatingThreadName;
        }

        public LockRequest build() {
            return (LockRequest) readResolve();
        }

        Object readResolve() {
            Builder builder = new Builder(lockMap).timeoutAfter(lockTimeout);
            if (lockGroupBehavior == LockGroupBehavior.LOCK_AS_MANY_AS_POSSIBLE) {
                builder.lockAsManyAsPossible();
            }
            if (blockingMode == BlockingMode.DO_NOT_BLOCK) {
                builder.doNotBlock();
            } else if (blockingMode == BlockingMode.BLOCK_UNTIL_TIMEOUT) {
                builder.blockForAtMost(blockingDuration);
            } else if (blockingMode == BlockingMode.BLOCK_INDEFINITELY_THEN_RELEASE) {
                builder.lockAndRelease();
            }
            if (versionId != null) {
                builder.withLockedInVersionId(versionId);
            }
            builder.withCreatingThreadName(creatingThreadName);
            return builder.build();
        }
    }
}
