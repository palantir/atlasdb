// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.lock;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

/**
 * A token representing a set of locks being held by a client and the
 * expiration date of the locks (leases). The expiration date can be extended by
 * refreshing the token with the lock server.
 *
 * @author jtamer
 */
@Immutable public final class HeldLocksToken implements ExpiringToken, Serializable {
    private static final long serialVersionUID = 0x99b3bb32bb98f83al;

    private final BigInteger tokenId;
    private final LockClient client;
    private final long creationDateMs;
    private final long expirationDateMs;
    private final SortedLockCollection<LockDescriptor> lockMap;
    private final SimpleTimeDuration lockTimeout;
    @Nullable private final Long versionId;

    /**
     * This should only be created by the Lock Service
     */
    public HeldLocksToken(BigInteger tokenId, LockClient client, long createionDateMs,
            long expirationDateMs, SortedLockCollection<LockDescriptor> lockMap,
            TimeDuration lockTimeout, @Nullable Long versionId) {
        this.tokenId = Preconditions.checkNotNull(tokenId);
        this.client = Preconditions.checkNotNull(client);
        this.creationDateMs = createionDateMs;
        this.expirationDateMs = expirationDateMs;
        this.lockMap = lockMap;
        this.lockTimeout = SimpleTimeDuration.of(lockTimeout);
        this.versionId = versionId;
        Preconditions.checkArgument(!this.lockMap.isEmpty());
    }

    /**
     * This should only be called from the Lock Service.
     */
    public BigInteger getTokenId() {
        return tokenId;
    }

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
    public SortedLockCollection<LockDescriptor> getLocks() {
        return lockMap;
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
    @Nullable public Long getVersionId() {
        return versionId;
    }

    @Override public boolean equals(@Nullable Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof HeldLocksToken)) {
            return false;
        }
        return tokenId.equals(((HeldLocksToken) obj).tokenId);
    }

    @Override public int hashCode() {
        return tokenId.hashCode();
    }

    @Override public String toString() {
        return toString(System.currentTimeMillis());
    }

    public String toString(long currentTimeMillis) {
        return MoreObjects.toStringHelper(this)
                .add("tokenId", tokenId.toString(Character.MAX_RADIX))
                .add("client", client)
                .add("createdAt", SimpleTimeDuration.of(creationDateMs, TimeUnit.MILLISECONDS))
                .add("expiresIn", SimpleTimeDuration.of(expirationDateMs - currentTimeMillis,
                        TimeUnit.MILLISECONDS))
                .add("lockCount", lockMap.size())
                .add("firstLock", lockMap.entries().iterator().next())
                .add("versionId", versionId)
                .toString();
    }

    /**
     * This should only be created by the lock service.  This call will not actually refresh the token.
     */
    public HeldLocksToken refresh(long expirationDateMs) {
        return new HeldLocksToken(tokenId, client, creationDateMs, expirationDateMs, lockMap, lockTimeout,
                versionId);
    }

    private void readObject(@SuppressWarnings("unused") ObjectInputStream in)
            throws InvalidObjectException {
        throw new InvalidObjectException("proxy required");
    }

    private Object writeReplace() {
        return new SerializationProxy(this);
    }

    private static class SerializationProxy implements Serializable {
        private static final long serialVersionUID = 0xe0bc169ce5a280acl;

        private final BigInteger tokenId;
        private final LockClient client;
        private final long creationDateMs;
        private final long expirationDateMs;
        private final SortedLockCollection<LockDescriptor> lockMap;
        private final SimpleTimeDuration lockTimeout;
        @Nullable private final Long versionId;

        SerializationProxy(HeldLocksToken heldLocksToken) {
            tokenId = heldLocksToken.tokenId;
            client = heldLocksToken.client;
            creationDateMs = heldLocksToken.creationDateMs;
            expirationDateMs = heldLocksToken.expirationDateMs;
            lockMap = heldLocksToken.lockMap;
            lockTimeout = heldLocksToken.lockTimeout;
            versionId = heldLocksToken.versionId;
        }

        Object readResolve() {
            return new HeldLocksToken(tokenId, client, creationDateMs, expirationDateMs, lockMap, lockTimeout,
                    versionId);
        }
    }
}
