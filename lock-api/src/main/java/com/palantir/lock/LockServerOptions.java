/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.lock;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * Provides the set of options which can be passed to the
 * lock server upon construction.
 *
 * @author jtamer
 */
@Immutable public class LockServerOptions implements Serializable {
    private static final long serialVersionUID = 0xb5fd04db6d65582al;

    /** The default lock server option values. */
    public static final LockServerOptions DEFAULT = new LockServerOptions();

    protected LockServerOptions() { /* empty */ }

    /**
     * Returns <code>true</code> if this is a standalone lock server or
     * <code>false</code> if the lock server code is running in-process with the only
     * client accessing it.
     */
    public boolean isStandaloneServer() {
        return true;
    }

    /**
     * Returns the maximum amount of time that can be passed to
     * {@link LockRequest.Builder#timeoutAfter(TimeDuration)}. The default value
     * is 10 minutes.
     */
    public TimeDuration getMaxAllowedLockTimeout() {
        return SimpleTimeDuration.of(10, TimeUnit.MINUTES);
    }

    /**
     * Returns the maximum permitted clock drift between the server and any
     * client. The default value is 5 seconds.
     */
    public TimeDuration getMaxAllowedClockDrift() {
        return SimpleTimeDuration.of(5, TimeUnit.SECONDS);
    }

    /**
     * Returns the maximum amount of time that may be passed to
     * {@link LockRequest.Builder#blockForAtMost(TimeDuration)}. The default
     * value is 60 seconds.
     */
    public TimeDuration getMaxAllowedBlockingDuration() {
        return SimpleTimeDuration.of(60, TimeUnit.SECONDS);
    }

    /**
     * Returns the number of bits used to create random lock token IDs. The
     * default value is 64 bits.
     */
    public int getRandomBitCount() {
        return Long.SIZE;
    }

    @Override public final boolean equals(@Nullable Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof LockServerOptions)) return false;
        LockServerOptions other = (LockServerOptions) obj;
        return Objects.equal(getMaxAllowedLockTimeout(), other.getMaxAllowedLockTimeout())
                && Objects.equal(getMaxAllowedClockDrift(), other.getMaxAllowedClockDrift())
                && (getRandomBitCount() == other.getRandomBitCount());
    }

    @Override public final int hashCode() {
        return Objects.hashCode(getMaxAllowedLockTimeout(), getMaxAllowedClockDrift(),
                getRandomBitCount());
    }

    @Override public final String toString() {
        return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("isStandaloneServer", isStandaloneServer())
                .add("maxAllowedLockTimeout", getMaxAllowedLockTimeout())
                .add("maxAllowedClockDrift", getMaxAllowedClockDrift())
                .add("maxAllowedBlockingDuration", getMaxAllowedBlockingDuration())
                .add("randomBitCount", getRandomBitCount())
                .toString();
    }

    private final void readObject(@SuppressWarnings("unused") ObjectInputStream in)
            throws InvalidObjectException {
        throw new InvalidObjectException("proxy required");
    }

    protected final Object writeReplace() {
        return new SerializationProxy(this);
    }

    private static class SerializationProxy implements Serializable {
        private static final long serialVersionUID = 0xece5944130b16002l;

        private final boolean isStandaloneServer;
        private final SimpleTimeDuration maxAllowedLockTimeout;
        private final SimpleTimeDuration maxAllowedClockDrift;
        private final SimpleTimeDuration maxAllowedBlockingDuration;
        private final int randomBitCount;

        SerializationProxy(LockServerOptions lockServerOptions) {
            isStandaloneServer = lockServerOptions.isStandaloneServer();
            maxAllowedLockTimeout = SimpleTimeDuration.of(
                    lockServerOptions.getMaxAllowedLockTimeout());
            maxAllowedClockDrift = SimpleTimeDuration.of(
                    lockServerOptions.getMaxAllowedClockDrift());
            maxAllowedBlockingDuration = SimpleTimeDuration.of(
                    lockServerOptions.getMaxAllowedBlockingDuration());
            randomBitCount = lockServerOptions.getRandomBitCount();
        }

        Object readResolve() {
            return new LockServerOptions() {
                private static final long serialVersionUID = 0xcc7124dcf06f0803l;
                @Override public boolean isStandaloneServer() {
                    return isStandaloneServer;
                }
                @Override public TimeDuration getMaxAllowedLockTimeout() {
                    return maxAllowedLockTimeout;
                }
                @Override public TimeDuration getMaxAllowedClockDrift() {
                    return maxAllowedClockDrift;
                }
                @Override public TimeDuration getMaxAllowedBlockingDuration() {
                    return maxAllowedBlockingDuration;
                }
                @Override public int getRandomBitCount() {
                    return randomBitCount;
                }
            };
        }
    }
}
