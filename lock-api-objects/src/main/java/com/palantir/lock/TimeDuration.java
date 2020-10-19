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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A container class to encapsulate a {@link TimeUnit} with a {@code long}
 * duration.
 *
 * @author jtamer
 */
@JsonDeserialize(as = SimpleTimeDuration.class)
public interface TimeDuration extends Comparable<TimeDuration> {

    long getTime();

    TimeUnit getUnit();

    long toNanos();

    long toMicros();

    long toMillis();

    long toSeconds();

    long toMinutes();

    long toHours();

    long toDays();

    long to(TimeUnit unit);

    void timedWait(Object lock) throws InterruptedException;

    void timedJoin(Thread thread) throws InterruptedException;

    void sleep() throws InterruptedException;

    /**
     * Two {@code TimeDuration}s are equal iff their nanosecond representations
     * are equal.
     */
    @Override
    boolean equals(@Nullable Object obj);

    /** Returns {@code com.google.common.base.Objects.hashCode(toNanos())}. */
    @Override
    int hashCode();
}
