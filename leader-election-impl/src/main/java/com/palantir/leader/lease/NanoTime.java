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

package com.palantir.leader.lease;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import com.google.common.annotations.VisibleForTesting;

public final class NanoTime implements Comparable<NanoTime> {
    private final long time;

    @VisibleForTesting
    NanoTime(long time) {
        this.time = time;
    }

    public static NanoTime now() {
        return new NanoTime(System.nanoTime());
    }

    /**
     * Sleep for a period of time, guaranteeing that the nanosecond precision clock has seen that
     * period of time elapsed.
     */
    public static void sleepUntil(NanoTime end) throws InterruptedException {
        NanoTime now = now();
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(end.time - now.time));
        while (end.time - (now = now()).time > 0) {
            LockSupport.parkNanos(end.time - now.time);
        }
    }

    public NanoTime plus(Duration duration) {
        return new NanoTime(time + duration.toNanos());
    }

    public boolean isBefore(NanoTime other) {
        return compareTo(other) < 0;
    }

    @Override
    public String toString() {
        return "NanoTime{" +
                "time=" + time +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NanoTime nanoTime = (NanoTime) o;
        return time == nanoTime.time;
    }

    @Override
    public int hashCode() {
        return Objects.hash(time);
    }

    @Override
    public int compareTo(NanoTime other) {
        if (other.time == time) {
            return 0;
        } else if (time - other.time > 0) { // this is the critical bit; now can wrap according to javadoc
            return 1;
        } else {
            return -1;
        }
    }
}
