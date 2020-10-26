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
import com.google.common.base.MoreObjects;
import com.google.common.primitives.Longs;
import com.palantir.logsafe.Preconditions;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * A simple, immutable implementation of {@link TimeDuration}.
 *
 * @author jtamer
 */
@Immutable
public final class SimpleTimeDuration implements TimeDuration, Serializable {
    private static final long serialVersionUID = 0x221d07e843433df7L;

    private final long time;
    private final TimeUnit unit;

    @JsonCreator
    public static SimpleTimeDuration of(@JsonProperty("time") long time, @JsonProperty("unit") TimeUnit unit) {
        return new SimpleTimeDuration(time, unit);
    }

    public static SimpleTimeDuration of(TimeDuration duration) {
        Preconditions.checkNotNull(duration, "duration should not be null");
        if (duration instanceof SimpleTimeDuration) {
            return (SimpleTimeDuration) duration;
        }
        return new SimpleTimeDuration(duration.getTime(), duration.getUnit());
    }

    private SimpleTimeDuration(long time, TimeUnit unit) {
        this.time = time;
        this.unit = Preconditions.checkNotNull(unit, "unit should not be null");
    }

    @Override
    public long getTime() {
        return time;
    }

    @Override
    public TimeUnit getUnit() {
        return unit;
    }

    @Override
    public long toNanos() {
        return unit.toNanos(time);
    }

    @Override
    public long toMicros() {
        return unit.toMicros(time);
    }

    @Override
    public long toMillis() {
        return unit.toMillis(time);
    }

    @Override
    public long toSeconds() {
        return unit.toSeconds(time);
    }

    @Override
    public long toMinutes() {
        return unit.toMinutes(time);
    }

    @Override
    public long toHours() {
        return unit.toHours(time);
    }

    @Override
    public long toDays() {
        return unit.toDays(time);
    }

    @Override
    public long to(TimeUnit timeUnit) {
        return timeUnit.convert(time, this.unit);
    }

    @Override
    public void timedWait(Object lock) throws InterruptedException {
        unit.timedWait(lock, time);
    }

    @Override
    public void timedJoin(Thread thread) throws InterruptedException {
        unit.timedJoin(thread, time);
    }

    @Override
    public void sleep() throws InterruptedException {
        unit.sleep(time);
    }

    @Override
    public int compareTo(TimeDuration other) {
        return Longs.compare(toNanos(), other.toNanos());
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof TimeDuration)) {
            return false;
        }
        return toNanos() == ((TimeDuration) obj).toNanos();
    }

    @Override
    public int hashCode() {
        return Long.hashCode(toNanos());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass().getSimpleName())
                .addValue(time + " " + unit.toString().toLowerCase())
                .toString();
    }

    private void readObject(@SuppressWarnings("unused") ObjectInputStream in) throws InvalidObjectException {
        throw new InvalidObjectException("proxy required");
    }

    private Object writeReplace() {
        return new SerializationProxy(this);
    }

    private static class SerializationProxy implements Serializable {
        private static final long serialVersionUID = 0xc60879b1484ec2cL;

        private final long time;
        private final TimeUnit unit;

        SerializationProxy(SimpleTimeDuration simpleTimeDuration) {
            time = simpleTimeDuration.time;
            unit = simpleTimeDuration.unit;
        }

        Object readResolve() {
            return of(time, unit);
        }
    }
}
