/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.timelock.copycat;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.timelock.atomix.ImmutableLeaderAndTerm;
import com.palantir.atlasdb.timelock.atomix.LeaderAndTerm;
import com.palantir.timestamp.TimestampRange;

import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachine;

public class TimeLockStateMachine extends StateMachine {
    private final ConcurrentMap<String, AtomicLong> timestampBounds;
    private final AtomicReference<LeaderAndTerm> clusterLeader;

    public TimeLockStateMachine() {
        timestampBounds = Maps.newConcurrentMap();
        clusterLeader = new AtomicReference<>();
    }

    public TimestampRange freshTimestamps(Commit<FreshTimestampsCommand> commit) {
        try {
            FreshTimestampsCommand command = commit.operation();
            timestampBounds.putIfAbsent(command.getNamespace(), new AtomicLong());
            long previousTimestamp = timestampBounds.get(command.getNamespace()).getAndAdd(command.getAmount());
            return TimestampRange.createInclusiveRange(previousTimestamp + 1, previousTimestamp + command.getAmount());
        } finally {
            commit.release();
        }
    }

    public LeaderAndTerm updateLeader(Commit<UpdateLeaderCommand> commit) {
        try {
            UpdateLeaderCommand command = commit.operation();
            while (true) {
                LeaderAndTerm currentLeader = clusterLeader.get();
                if (currentLeader != null && currentLeader.term() >= command.term()) {
                    return currentLeader;
                }
                if (clusterLeader.compareAndSet(currentLeader,
                        ImmutableLeaderAndTerm.of(command.term(), String.valueOf(command.leader())))) {
                    return currentLeader;
                }
            }
        } finally {
            commit.release();
        }
    }

    public LeaderAndTerm getLeaderInfo(Commit<GetLeaderQuery> commit) {
        try {
            return clusterLeader.get();
        } finally {
            commit.release();
        }
    }
}
