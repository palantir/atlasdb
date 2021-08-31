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
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * A token representing a set of locks being held by a client and the
 * expiration date of the locks (leases). The expiration date can be extended by
 * refreshing the token with the lock server.
 *
 * @author jtamer
 */
@JsonDeserialize(builder = HeldLocksToken.SerializationProxy.class)
@Immutable
public final class HeldLocksToken implements ExpiringToken, Serializable {
    private static final long serialVersionUID = 0x99b3bb32bb98f83aL;

    private final BigInteger tokenId;
    private final LockClient client;
    private final long creationDateMs;
    private final long expirationDateMs;
    private final SortedLockCollection<LockDescriptor> lockMap;
    private final SimpleTimeDuration lockTimeout;

    @Nullable
    private final Long versionId;

    private final String requestingThread;

    /**
     * This should only be created by the Lock Service.
     */
    public HeldLocksToken(
            BigInteger tokenId,
            LockClient client,
            long creationDateMs,
            long expirationDateMs,
            SortedLockCollection<LockDescriptor> lockMap,
            TimeDuration lockTimeout,
            @Nullable Long versionId,
            String requestingThread) {
        this.tokenId = Preconditions.checkNotNull(tokenId, "tokenId should not be null");
        this.client = Preconditions.checkNotNull(client, "client should not be null");
        this.creationDateMs = creationDateMs;
        this.expirationDateMs = expirationDateMs;
        this.lockMap = lockMap;
        this.lockTimeout = SimpleTimeDuration.of(lockTimeout);
        this.versionId = versionId;
        this.requestingThread = requestingThread;
        Preconditions.checkArgument(!this.lockMap.isEmpty());
    }

    /**
     * This should only be called from the Lock Service.
     */
    public BigInteger getTokenId() {
        return tokenId;
    }

    @JsonIgnore
    public LockRefreshToken getLockRefreshToken() {
        return new LockRefreshToken(tokenId, expirationDateMs);
    }

    /** Returns the client who holds these locks. */
    @Override
    public LockClient getClient() {
        return client;
    }

    /**
     * Returns the time (in milliseconds since the epoch) since this token was
     * created.
     */
    @Override
    public long getCreationDateMs() {
        return creationDateMs;
    }

    public String getRequestingThread() {
        return requestingThread;
    }

    /**
     * Returns the time (in milliseconds since the epoch) when this token will
     * expire and become invalid.
     */
    @Override
    public long getExpirationDateMs() {
        return expirationDateMs;
    }

    /**
     * Returns the set of locks which were successfully acquired as a map
     * from descriptor to lock mode.
     */
    @JsonIgnore
    @Override
    public SortedLockCollection<LockDescriptor> getLockDescriptors() {
        return lockMap;
    }

    public List<LockWithMode> getLocks() {
        return ImmutableList.copyOf(Iterables.transform(lockMap.entries(), HeldLocksToken::toLockWithModeFunction));
    }

    /**
     * Returns the amount of time that it takes for these locks to
     * expire.
     */
    @Override
    public TimeDuration getLockTimeout() {
        return lockTimeout;
    }

    /**
     * Returns the version ID for this token, or {@code null} if no version ID
     * was specified.
     */
    @Override
    @Nullable
    public Long getVersionId() {
        return versionId;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof HeldLocksToken)) {
            return false;
        }
        return tokenId.equals(((HeldLocksToken) obj).tokenId);
    }

    @Override
    public int hashCode() {
        return tokenId.hashCode();
    }

    @Override
    public String toString() {
        return toString(System.currentTimeMillis());
    }

    public String toString(long currentTimeMillis) {
        return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("tokenId", tokenId.toString(Character.MAX_RADIX))
                .add("client", client)
                .add("createdAt", SimpleTimeDuration.of(creationDateMs, TimeUnit.MILLISECONDS))
                .add("expiresIn", SimpleTimeDuration.of(expirationDateMs - currentTimeMillis, TimeUnit.MILLISECONDS))
                .add("lockCount", lockMap.size())
                .add("firstLock", lockMap.entries().iterator().next())
                .add("versionId", versionId)
                .add("requestingThread", requestingThread)
                .toString();
    }

    /**
     * This should only be created by the lock service.  This call will not actually refresh the token.
     */
    public HeldLocksToken refresh(long newExpirationDateMs) {
        return new HeldLocksToken(
                tokenId,
                client,
                creationDateMs,
                newExpirationDateMs,
                lockMap,
                lockTimeout,
                versionId,
                requestingThread);
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

    static class SerializationProxy implements Serializable {
        private static final long serialVersionUID = 0xe0bc169ce5a280acL;

        private final BigInteger tokenId;
        private final LockClient client;
        private final long creationDateMs;
        private final long expirationDateMs;
        private final SortedLockCollection<LockDescriptor> lockMap;
        private final SimpleTimeDuration lockTimeout;

        @Nullable
        private final Long versionId;

        private final String requestingThread;

        SerializationProxy(HeldLocksToken heldLocksToken) {
            tokenId = heldLocksToken.tokenId;
            client = heldLocksToken.client;
            creationDateMs = heldLocksToken.creationDateMs;
            expirationDateMs = heldLocksToken.expirationDateMs;
            lockMap = heldLocksToken.lockMap;
            lockTimeout = heldLocksToken.lockTimeout;
            versionId = heldLocksToken.versionId;
            requestingThread = heldLocksToken.requestingThread;
        }

        @JsonCreator
        SerializationProxy(
                @JsonProperty("tokenId") BigInteger tokenId,
                @JsonProperty("client") LockClient client,
                @JsonProperty("creationDateMs") long creationDateMs,
                @JsonProperty("expirationDateMs") long expirationDateMs,
                @JsonProperty("locks") List<LockWithMode> locks,
                @JsonProperty("lockTimeout") TimeDuration lockTimeout,
                @JsonProperty("versionId") Long versionId,
                @JsonProperty("requestingThread") String requestingThread) {
            ImmutableSortedMap.Builder<LockDescriptor, LockMode> localLockMapBuilder =
                    ImmutableSortedMap.naturalOrder();
            for (LockWithMode lock : locks) {
                localLockMapBuilder.put(lock.getLockDescriptor(), lock.getLockMode());
            }
            this.lockMap = LockCollections.of(localLockMapBuilder.build());
            this.tokenId = Preconditions.checkNotNull(tokenId, "tokenId");
            this.client = Preconditions.checkNotNull(client, "client");
            this.creationDateMs = creationDateMs;
            this.expirationDateMs = expirationDateMs;
            this.lockTimeout = SimpleTimeDuration.of(lockTimeout);
            this.versionId = versionId;
            this.requestingThread = requestingThread;
            Preconditions.checkArgument(!this.lockMap.isEmpty());
        }

        public HeldLocksToken build() {
            return (HeldLocksToken) readResolve();
        }

        Object readResolve() {
            return new HeldLocksToken(
                    tokenId,
                    client,
                    creationDateMs,
                    expirationDateMs,
                    lockMap,
                    lockTimeout,
                    versionId,
                    requestingThread);
        }
    }
}
