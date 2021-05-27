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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.MoreObjects;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * Provides the set of options which can be passed to the
 * lock server upon construction.
 *
 * @author jtamer
 */
@JsonDeserialize(builder = LockServerOptions.SerializationProxy.class)
@Value.Immutable
@SuppressWarnings("ClassInitializationDeadlock")
public class LockServerOptions implements Serializable {
    private static final long serialVersionUID = 2930574230723753879L;

    @Deprecated
    public static final LockServerOptions DEFAULT = LockServerOptions.builder().build();

    /**
     * Returns <code>true</code> if this is a standalone lock server or
     * <code>false</code> if the lock server code is running in-process with the only
     * client accessing it.
     */
    @Value.Default
    public boolean isStandaloneServer() {
        return true;
    }

    /**
     * Returns the maximum amount of time that can be passed to
     * {@link LockRequest.Builder#timeoutAfter(TimeDuration)}. The default value
     * is 10 minutes.
     */
    @Value.Default
    public TimeDuration getMaxAllowedLockTimeout() {
        return SimpleTimeDuration.of(10, TimeUnit.MINUTES);
    }

    /**
     * Returns the maximum permitted clock drift between the server and any
     * client. The default value is 5 seconds.
     */
    @Value.Default
    public TimeDuration getMaxAllowedClockDrift() {
        return SimpleTimeDuration.of(5, TimeUnit.SECONDS);
    }

    /**
     * Returns the maximum amount of time that may be passed to
     * {@link LockRequest.Builder#blockForAtMost(TimeDuration)}. The default
     * value is 60 seconds.
     *
     * @deprecated this value is no longer used or respected.
     */
    @Deprecated
    @SuppressWarnings("InlineMeSuggester")
    @Value.Default
    public TimeDuration getMaxAllowedBlockingDuration() {
        return SimpleTimeDuration.of(60, TimeUnit.SECONDS);
    }

    /**
     * Returns the maximum amount of time a lock is usually held for.
     * The default value is 1 hour.
     */
    @Value.Default
    public TimeDuration getMaxNormalLockAge() {
        return SimpleTimeDuration.of(1, TimeUnit.HOURS);
    }

    /**
     * Returns the number of bits used to create random lock token IDs. The
     * default value is 64 bits.
     *
     * @deprecated this value is no longer used or respected
     */
    @Deprecated
    @SuppressWarnings("InlineMeSuggester")
    @Value.Default
    public int getRandomBitCount() {
        return Long.SIZE;
    }

    /**
     * This setting should equal or exceed the Atlas transaction timeout.
     */
    @Value.Default
    public TimeDuration getStuckTransactionTimeout() {
        return SimpleTimeDuration.of(1, TimeUnit.DAYS);
    }

    /**
     * Warn level logging for any lock request that receives a response after given time.
     * If the duration is zero or negative, slow lock logging will be disabled.
     */
    @JsonProperty("slowLogTriggerMillis")
    @Value.Default
    public long slowLogTriggerMillis() {
        return 10000L;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LockServerOptions)) {
            return false;
        }
        LockServerOptions other = (LockServerOptions) obj;
        return isStandaloneServer() == other.isStandaloneServer()
                && slowLogTriggerMillis() == other.slowLogTriggerMillis()
                && getRandomBitCount() == other.getRandomBitCount()
                && Objects.equals(getMaxAllowedLockTimeout(), other.getMaxAllowedLockTimeout())
                && Objects.equals(getMaxAllowedClockDrift(), other.getMaxAllowedClockDrift())
                && Objects.equals(getMaxAllowedBlockingDuration(), other.getMaxAllowedBlockingDuration())
                && Objects.equals(getMaxNormalLockAge(), other.getMaxNormalLockAge())
                && Objects.equals(getStuckTransactionTimeout(), other.getStuckTransactionTimeout())
                && Objects.equals(getLockStateLoggerDir(), other.getLockStateLoggerDir());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                isStandaloneServer(),
                getMaxAllowedLockTimeout(),
                getMaxAllowedClockDrift(),
                getMaxAllowedBlockingDuration(),
                getMaxNormalLockAge(),
                getRandomBitCount(),
                getStuckTransactionTimeout(),
                getLockStateLoggerDir(),
                slowLogTriggerMillis());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("isStandaloneServer", isStandaloneServer())
                .add("maxAllowedLockTimeout", getMaxAllowedLockTimeout())
                .add("maxAllowedClockDrift", getMaxAllowedClockDrift())
                .add("maxAllowedBlockingDuration", getMaxAllowedBlockingDuration())
                .add("maxNormalLockAge", getMaxNormalLockAge())
                .add("randomBitCount", getRandomBitCount())
                .add("stuckTransactionTimeout", getStuckTransactionTimeout())
                .add("lockStateLoggerDir", getLockStateLoggerDir())
                .add("slowLogTriggerMillis", slowLogTriggerMillis())
                .toString();
    }

    private void readObject(@SuppressWarnings("unused") ObjectInputStream in) throws InvalidObjectException {
        throw new InvalidObjectException("proxy required");
    }

    protected Object writeReplace() {
        return new SerializationProxy(this);
    }

    @Value.Default
    public String getLockStateLoggerDir() {
        return "log/state";
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder extends ImmutableLockServerOptions.Builder {}

    static class SerializationProxy implements Serializable {
        private static final long serialVersionUID = 4043798817916565364L;

        private final boolean isStandaloneServer;
        private final SimpleTimeDuration maxAllowedLockTimeout;
        private final SimpleTimeDuration maxAllowedClockDrift;
        private final SimpleTimeDuration maxAllowedBlockingDuration;
        private final SimpleTimeDuration maxNormalLockAge;
        private final int randomBitCount;
        private final SimpleTimeDuration stuckTransactionTimeout;
        private final String lockStateLoggerDir;
        private final long slowLogTriggerMillis;

        SerializationProxy(LockServerOptions lockServerOptions) {
            isStandaloneServer = lockServerOptions.isStandaloneServer();
            maxAllowedLockTimeout = SimpleTimeDuration.of(lockServerOptions.getMaxAllowedLockTimeout());
            maxAllowedClockDrift = SimpleTimeDuration.of(lockServerOptions.getMaxAllowedClockDrift());
            maxAllowedBlockingDuration = SimpleTimeDuration.of(lockServerOptions.getMaxAllowedBlockingDuration());
            maxNormalLockAge = SimpleTimeDuration.of(lockServerOptions.getMaxNormalLockAge());
            randomBitCount = lockServerOptions.getRandomBitCount();
            stuckTransactionTimeout = SimpleTimeDuration.of(lockServerOptions.getStuckTransactionTimeout());
            lockStateLoggerDir = lockServerOptions.getLockStateLoggerDir();
            slowLogTriggerMillis = lockServerOptions.slowLogTriggerMillis();
        }

        @JsonCreator
        SerializationProxy(
                @JsonProperty("isStandaloneServer") boolean isStandaloneServer,
                @JsonProperty("maxAllowedLockTimeout") SimpleTimeDuration maxAllowedLockTimeout,
                @JsonProperty("maxAllowedClockDrift") SimpleTimeDuration maxAllowedClockDrift,
                @JsonProperty("maxAllowedBlockingDuration") SimpleTimeDuration maxAllowedBlockingDuration,
                @JsonProperty("maxNormalLockAge") SimpleTimeDuration maxNormalLockAge,
                @JsonProperty("stuckTransactionTimeout") SimpleTimeDuration stuckTransactionTimeout,
                @JsonProperty("randomBitCount") int randomBitCount,
                @JsonProperty("lockStateLoggerDir") String lockStateLoggerDir,
                @JsonProperty("slowLogTriggerMillis") long slowLogTriggerMillis) {
            this.isStandaloneServer = isStandaloneServer;
            this.maxAllowedLockTimeout = maxAllowedLockTimeout;
            this.maxAllowedClockDrift = maxAllowedClockDrift;
            this.maxAllowedBlockingDuration = maxAllowedBlockingDuration;
            this.maxNormalLockAge = maxNormalLockAge;
            this.randomBitCount = randomBitCount;
            this.stuckTransactionTimeout = stuckTransactionTimeout;
            this.lockStateLoggerDir = lockStateLoggerDir;
            this.slowLogTriggerMillis = slowLogTriggerMillis;
        }

        public LockServerOptions build() {
            return (LockServerOptions) readResolve();
        }

        Object readResolve() {
            return new Builder()
                    .isStandaloneServer(isStandaloneServer)
                    .maxAllowedLockTimeout(maxAllowedLockTimeout)
                    .maxAllowedClockDrift(maxAllowedClockDrift)
                    .maxAllowedBlockingDuration(maxAllowedBlockingDuration)
                    .maxNormalLockAge(maxNormalLockAge)
                    .randomBitCount(randomBitCount)
                    .stuckTransactionTimeout(stuckTransactionTimeout)
                    .lockStateLoggerDir(lockStateLoggerDir)
                    .slowLogTriggerMillis(slowLogTriggerMillis)
                    .build();
        }
    }
}
