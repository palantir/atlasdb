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

package com.palantir.paxos;

import com.google.common.net.HostAndPort;
import com.palantir.leader.PaxosLeaderElectionEventRecorder;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.derive4j.Data;

@Data
public abstract class LeaderPingResult {

    interface Cases<R> {
        R pingTimedOut();
        R pingReturnedTrue(UUID leaderUuid, HostAndPort leader);
        R pingReturnedFalse();
        R pingCallFailure(Throwable exception);
    }

    public abstract <R> R match(Cases<R> cases);

    public void recordEvent(PaxosLeaderElectionEventRecorder eventRecorder) {
        LeaderPingResults.caseOf(this)
                .pingTimedOut(wrap(eventRecorder::recordLeaderPingTimeout))
                .pingReturnedFalse(wrap(eventRecorder::recordLeaderPingReturnedFalse))
                .pingCallFailure(wrap(eventRecorder::recordLeaderPingFailure))
                .otherwise_(null);
    }

    public boolean isSuccessful() {
        return LeaderPingResults.caseOf(this)
                .pingReturnedTrue_(true)
                .otherwise_(false);
    }

    private static <R> Supplier<R> wrap(Runnable runnable) {
        return () -> {
            runnable.run();
            return null;
        };
    }

    private static <T, R> Function<T, R> wrap(Consumer<T> consumer) {
        return t -> {
            consumer.accept(t);
            return null;
        };
    }

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract String toString();

}
