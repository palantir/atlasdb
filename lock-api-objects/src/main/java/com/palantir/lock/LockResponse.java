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
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.logsafe.Preconditions;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * Represents the result of calling {@link LockService#lockWithFullLockResponse(LockClient, LockRequest)} on
 * the lock server. If locks were successfully acquired, then {@link #success()}
 * returns <code>true</code> and {@link #getToken()} returns the token which
 * represents the held locks.
 *
 * @author jtamer
 */
@JsonDeserialize(builder = LockResponse.SerializationProxy.class)
@Immutable
public final class LockResponse implements Serializable {
    private static final long serialVersionUID = 0xd67972b13e30eff7L;

    @Nullable
    private final HeldLocksToken token;

    private final boolean isBlockAndRelease;
    private final ImmutableSortedMap<LockDescriptor, LockClient> lockHolders;

    public LockResponse(@Nullable HeldLocksToken token) {
        this(token, ImmutableSortedMap.of());
    }

    /**
     * This should only get created by the Lock Service.
     */
    public LockResponse(@Nullable HeldLocksToken token, Map<LockDescriptor, LockClient> lockHolders) {
        this.token = token;
        this.lockHolders = ImmutableSortedMap.copyOf(lockHolders);
        isBlockAndRelease = false;
        Preconditions.checkArgument(token != null || !this.lockHolders.isEmpty());
    }

    /**
     * This should only get created by the Lock Service.
     *
     * This constructor is for {@link BlockingMode#BLOCK_INDEFINITELY_THEN_RELEASE}
     */
    public LockResponse(Map<LockDescriptor, LockClient> lockHolders) {
        this.token = null;
        this.lockHolders = ImmutableSortedMap.copyOf(lockHolders);
        assert this.lockHolders.isEmpty();
        isBlockAndRelease = true;
    }

    /**
     * Creates a new, successful {@code LockResponse} wrapping the given token,
     * with an empty lock holders map.
     */
    public static LockResponse createSuccessful(HeldLocksToken token) {
        return new LockResponse(Preconditions.checkNotNull(token), ImmutableSortedMap.<LockDescriptor, LockClient>of());
    }

    /**
     * Returns <code>true</code> if locks were acquired. Note that if the lock
     * request specified {@link BlockingMode#BLOCK_INDEFINITELY} (the default
     * behavior) or {@link BlockingMode#BLOCK_INDEFINITELY_THEN_RELEASE},
     * then this method is guaranteed to return <code>true</code>.
     */
    public boolean success() {
        if (isBlockAndRelease) {
            return lockHolders.isEmpty();
        }
        return token != null;
    }

    /**
     * If {@link #success()} is <code>true</code>, then this method returns a token
     * representing the held locks; otherwise, this method returns <code>null</code>.
     */
    @Nullable
    public HeldLocksToken getToken() {
        return token;
    }

    @JsonIgnore
    @Nullable
    public LockRefreshToken getLockRefreshToken() {
        return token == null ? null : token.getLockRefreshToken();
    }

    public boolean isBlockAndRelease() {
        return isBlockAndRelease;
    }

    public List<LockWithClient> getLocks() {
        return lockHolders.entrySet().stream()
                .map(input -> ImmutableLockWithClient.builder()
                        .lockDescriptor(input.getKey())
                        .lockClient(input.getValue())
                        .build())
                .collect(Collectors.toList());
    }

    /**
     * Returns a map of lock descriptors and lock clients. Each entry's key is a
     * lock which could not be acquired by the lock server, and its value is one
     * of the clients who was already holding the lock. If multiple clients are
     * holding a read lock, then one of those clients is picked arbitrarily to
     * be used as the value in this map.
     * <p>
     * Note that this map will not necessarily contain every lock which could
     * not be acquired; however, if at least one of the requested locks was not
     * acquired, then this map is guaranteed to contain at least one entry. Also
     * note that even if {@link #success()} returns <code>true</code>, this map might
     * not be empty if the lock request specified
     * {@link LockGroupBehavior#LOCK_AS_MANY_AS_POSSIBLE}.
     */
    @JsonIgnore
    public ImmutableSortedMap<LockDescriptor, LockClient> getLockHolders() {
        return lockHolders;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LockResponse)) {
            return false;
        }
        return token.equals(((LockResponse) obj).token)
                && lockHolders.equals(((LockResponse) obj).getLockHolders())
                && isBlockAndRelease == ((LockResponse) obj).isBlockAndRelease();
    }

    @Override
    public int hashCode() {
        return Objects.hash(token, lockHolders, isBlockAndRelease);
    }

    public String toString(long currentTimeMillis) {
        return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("token", token.toString(currentTimeMillis))
                .add("lockHolders", lockHolders)
                .add("isBlockAndRelease", isBlockAndRelease)
                .toString();
    }

    private void readObject(@SuppressWarnings("unused") ObjectInputStream in) throws InvalidObjectException {
        throw new InvalidObjectException("proxy required");
    }

    private Object writeReplace() {
        return new SerializationProxy(this);
    }

    static class SerializationProxy implements Serializable {
        private static final long serialVersionUID = 0xcff22b33b08dd857L;

        @Nullable
        private final HeldLocksToken token;

        private final ImmutableSortedMap<LockDescriptor, LockClient> lockHolders;
        private final boolean isBlockAndRelease;

        SerializationProxy(LockResponse lockResponse) {
            this.token = lockResponse.token;
            this.lockHolders = lockResponse.lockHolders;
            this.isBlockAndRelease = lockResponse.isBlockAndRelease;
        }

        @JsonCreator
        SerializationProxy(
                @JsonProperty("token") HeldLocksToken token,
                @JsonProperty("locks") List<LockWithClient> lockWithClients,
                @JsonProperty("blockAndRelease") boolean isBlockAndRelease) {
            if (lockWithClients == null) {
                lockHolders = ImmutableSortedMap.of();
            } else {
                ImmutableSortedMap.Builder<LockDescriptor, LockClient> lockHoldersBuilder =
                        ImmutableSortedMap.naturalOrder();
                for (LockWithClient lock : lockWithClients) {
                    lockHoldersBuilder.put(lock.getLockDescriptor(), lock.getLockClient());
                }
                this.lockHolders = lockHoldersBuilder.build();
            }
            this.token = token;
            this.isBlockAndRelease = isBlockAndRelease;
        }

        public LockResponse build() {
            return (LockResponse) readResolve();
        }

        Object readResolve() {
            if (isBlockAndRelease) {
                return new LockResponse(lockHolders);
            }
            return new LockResponse(token, lockHolders);
        }
    }
}
