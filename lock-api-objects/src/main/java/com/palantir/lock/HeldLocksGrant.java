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
import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.palantir.logsafe.Preconditions;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * A grant for a set of locks which are currently held by the lock server. Lock
 * grants are created by the {@link LockService#convertToGrant(HeldLocksToken)}
 * method and can be exchanged for ownership of the locks by the
 * {@link LockService#useGrant(LockClient, HeldLocksGrant)}.
 *
 * @author jtamer
 */
@JsonDeserialize(builder =
        HeldLocksGrant.SerializationProxy.class)
@Immutable public final class HeldLocksGrant implements ExpiringToken, Serializable {
    private static final long serialVersionUID = 0xcdf42e080ef965dcL;

    private static final Function<Map.Entry<LockDescriptor, LockMode>, LockWithMode> TO_LOCK_WITH_MODE_FUNCTION =
            input -> new LockWithMode(input.getKey(), input.getValue());

    private final BigInteger grantId;
    private final long creationDateMs;
    private final long expirationDateMs;
    private final SortedLockCollection<LockDescriptor> lockMap;
    @Nullable private final SimpleTimeDuration lockTimeout;
    @Nullable private final Long versionId;

    /**
     * These grants should not be constructed by users.  Only the lock service should hand them out.
     */
    public HeldLocksGrant(BigInteger grantId) {
        this.grantId = Preconditions.checkNotNull(grantId, "grantId should not be null");
        creationDateMs = System.currentTimeMillis();
        expirationDateMs = -1;
        lockMap = LockCollections.of();
        lockTimeout = null;
        versionId = null;
    }

    /**
     * These grants should not be constructed by users.  Only the lock service should hand them out.
     */
    public HeldLocksGrant(BigInteger grantId, long creationDateMs, long expirationDateMs,
            SortedLockCollection<LockDescriptor> lockMap, TimeDuration lockTimeout,
            @Nullable Long versionId) {
        this.grantId = Preconditions.checkNotNull(grantId, "grantId should not be null");
        this.creationDateMs = creationDateMs;
        this.expirationDateMs = expirationDateMs;
        this.lockMap = lockMap;
        this.lockTimeout = SimpleTimeDuration.of(lockTimeout);
        this.versionId = versionId;
        Preconditions.checkArgument(!this.lockMap.isEmpty());
    }

    /** Returns the grant ID. */
    public BigInteger getGrantId() {
        return grantId;
    }

    /** Always returns {@code null}. Lock grants are not held by a client. */
    @JsonIgnore
    @Override
    @Nullable public LockClient getClient() {
        return null;
    }

    @JsonIgnore
    @Override
    public SortedLockCollection<LockDescriptor> getLockDescriptors() {
        return getLocks();
    }

    /**
     * Returns the time (in milliseconds since the epoch) since this token was
     * created.
     */
    @Override
    public long getCreationDateMs() {
        return creationDateMs;
    }

    /**
     * Returns the time, in milliseconds since the epoch, when this token will
     * expire and become invalid.
     */
    @Override
    public long getExpirationDateMs() {
        Preconditions.checkState(expirationDateMs != -1);
        return expirationDateMs;
    }

    /**
     * Returns the set of locks which were successfully acquired, as a mapping
     * from descriptor to lock mode.
     */
    @JsonIgnore
    public SortedLockCollection<LockDescriptor> getLocks() {
        Preconditions.checkState(!lockMap.isEmpty());
        return lockMap;
    }

    public List<LockWithMode> getLocksWithMode() {
        return ImmutableList.copyOf(Iterables.transform(lockMap.entries(), TO_LOCK_WITH_MODE_FUNCTION));
    }

    /**
     * Returns the amount of time that it takes for these locks to
     * expire.
     */
    @Override
    public TimeDuration getLockTimeout() {
        Preconditions.checkState(lockTimeout != null);
        return lockTimeout;
    }

    /**
     * Returns the version ID for this token, or {@code null} if no version ID
     * was specified.
     */
    @Override
    @Nullable public Long getVersionId() {
        return versionId;
    }

    @Override public boolean equals(@Nullable Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof HeldLocksGrant)) {
            return false;
        }
        return grantId.equals(((HeldLocksGrant) obj).grantId);
    }

    @Override public int hashCode() {
        return grantId.hashCode();
    }

    @Override public String toString() {
        return toString(System.currentTimeMillis());
    }

    public String toString(long currentTimeMillis) {
        return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("grantId", grantId.toString(Character.MAX_RADIX))
                .add("createdAt", SimpleTimeDuration.of(creationDateMs, TimeUnit.MILLISECONDS))
                .add("expiresIn", SimpleTimeDuration.of(expirationDateMs,
                        TimeUnit.MILLISECONDS))
                .add("lockCount", lockMap.size())
                .add("firstLock", lockMap.entries().iterator().next())
                .add("versionId", versionId)
                .toString();
    }

    /**
     * This should only be called by the Lock Service.  Calling this method won't actually refresh the grant.
     */
    public HeldLocksGrant refresh(long newExpirationDateMs) {
        return new HeldLocksGrant(grantId, creationDateMs, newExpirationDateMs, lockMap, lockTimeout, versionId);
    }

    private void readObject(@SuppressWarnings("unused") ObjectInputStream in)
            throws InvalidObjectException {
        throw new InvalidObjectException("proxy required");
    }

    private Object writeReplace() {
        return new SerializationProxy(this);
    }

    static class SerializationProxy implements Serializable {
        private static final long serialVersionUID = 0xb9d2975ea14a7762L;

        private final BigInteger grantId;
        private final long creationDateMs;
        private final long expirationDateMs;
        private final SortedLockCollection<LockDescriptor> lockMap;
        @Nullable private final SimpleTimeDuration lockTimeout;
        @Nullable private final Long versionId;

        SerializationProxy(HeldLocksGrant heldLocksGrant) {
            grantId = heldLocksGrant.grantId;
            creationDateMs = heldLocksGrant.creationDateMs;
            expirationDateMs = heldLocksGrant.expirationDateMs;
            lockMap = heldLocksGrant.lockMap;
            lockTimeout = heldLocksGrant.lockTimeout;
            versionId = heldLocksGrant.versionId;
        }

        @JsonCreator
        SerializationProxy(@JsonProperty("grantId") BigInteger grantId,
                @JsonProperty("creationDateMs") long creationDateMs,
                @JsonProperty("expirationDateMs") long expirationDateMs,
                @JsonProperty("locksWithMode") List<LockWithMode> lockMap,
                @JsonProperty("lockTimeout") SimpleTimeDuration lockTimeout,
                @JsonProperty("versionId") Long versionId) {
            this.grantId = grantId;
            this.creationDateMs = creationDateMs;
            this.expirationDateMs = expirationDateMs;
            ImmutableSortedMap.Builder<LockDescriptor, LockMode> localLockMapBuilder =
                    ImmutableSortedMap.naturalOrder();
            for (LockWithMode lock : lockMap) {
                localLockMapBuilder.put(lock.getLockDescriptor(), lock.getLockMode());
            }
            this.lockMap = LockCollections.of(localLockMapBuilder.build());
            this.lockTimeout = lockTimeout;
            this.versionId = versionId;
        }

        public HeldLocksGrant build() {
            return (HeldLocksGrant) readResolve();
        }

        Object readResolve() {
            return lockMap.isEmpty() ? new HeldLocksGrant(grantId)
                    : new HeldLocksGrant(grantId, creationDateMs, expirationDateMs, lockMap, lockTimeout, versionId);
        }
    }
}
