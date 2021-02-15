/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.common.time;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.time.Duration;
import java.util.concurrent.locks.LockSupport;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
public abstract class NanoTime implements Comparable<NanoTime> {
    @JsonValue
    abstract long time();

    public static NanoTime now() {
        return create(System.nanoTime());
    }

    public static NanoTime createForTests(long nanos) {
        return create(nanos);
    }

    @JsonCreator
    static NanoTime create(long nanos) {
        return ImmutableNanoTime.builder().time(nanos).build();
    }

    /**
     * Sleep for a period of time, guaranteeing that the nanosecond precision clock has seen that
     * period of time elapsed.
     */
    public static void sleepUntil(NanoTime end) throws InterruptedException {
        for (NanoTime now = now(); nanosBetween(now, end) > 0; now = now()) {
            LockSupport.parkNanos(nanosBetween(now, end));
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }
    }

    private static long nanosBetween(NanoTime first, NanoTime second) {
        return second.time() - first.time();
    }

    public NanoTime plus(Duration duration) {
        return create(time() + duration.toNanos());
    }

    public boolean isBefore(NanoTime other) {
        return compareTo(other) < 0;
    }

    @Override
    public int compareTo(NanoTime other) {
        if (other.time() == time()) {
            return 0;
        } else if (time() - other.time() > 0) { // this is the critical bit; now can wrap according to javadoc
            return 1;
        } else {
            return -1;
        }
    }
}
