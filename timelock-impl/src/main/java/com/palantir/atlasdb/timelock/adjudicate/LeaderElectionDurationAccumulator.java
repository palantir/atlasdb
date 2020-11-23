/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.adjudicate;

import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.timelock.feedback.LeaderElectionDuration;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongConsumer;
import org.immutables.value.Value;

public class LeaderElectionDurationAccumulator {
    private final Set<LeadersContext> alreadyReportedLeaderElections = ConcurrentHashMap.newKeySet();
    private final Map<LeadersContext, SoakingDuration> currentlySoaking = new ConcurrentHashMap<>();

    private final LongConsumer updateConsumer;
    private final int updatesToAchieveConfidence;

    /**
     * This class accumulates {@link LeaderElectionDuration}s by tracking the shortest duration observed for each pair
     * of leader ids until updatesToAchieveConfidence have been received for that pair. Once that occurs, the
     * final duration is consumed by the consumer, and further updates for the leader id pair are ignored.
     *
     * @param updatesToAchieveConfidence required number of updates to before the results are reported, must be
     *                                   greater than 1
     */
    public LeaderElectionDurationAccumulator(LongConsumer updateConsumer, int updatesToAchieveConfidence) {
        this.updateConsumer = updateConsumer;
        this.updatesToAchieveConfidence = updatesToAchieveConfidence;
        Preconditions.checkArgument(
                updatesToAchieveConfidence > 1,
                "Number of required updates must be greater than 1.",
                SafeArg.of("updatesToAchieveConfidence", updatesToAchieveConfidence));
    }

    public void add(LeaderElectionDuration electionDuration) {
        LeadersContext leadersContext = LeadersContext.builder()
                .oldLeader(electionDuration.getOldLeader())
                .newLeader(electionDuration.getNewLeader())
                .build();
        if (!alreadyReportedLeaderElections.contains(leadersContext)) {
            currentlySoaking.compute(
                    leadersContext,
                    (context, previous) -> increaseConfidence(context, previous, electionDuration.getDuration()));
        }
    }

    private SoakingDuration increaseConfidence(
            LeadersContext context, SoakingDuration accumulatedSoakingDuration, Duration duration) {
        if (alreadyReportedLeaderElections.contains(context)) {
            return null;
        }

        if (accumulatedSoakingDuration == null) {
            return SoakingDuration.create(duration);
        }

        SoakingDuration updatedDuration = accumulatedSoakingDuration.updateDuration(duration);
        if (updatedDuration.count() >= updatesToAchieveConfidence) {
            alreadyReportedLeaderElections.add(context);
            updateConsumer.accept(updatedDuration.sampleMinimum());
            return null;
        }

        return updatedDuration;
    }

    @Value.Immutable
    interface LeadersContext {
        static ImmutableLeadersContext.Builder builder() {
            return ImmutableLeadersContext.builder();
        }

        UUID oldLeader();

        UUID newLeader();
    }

    @Value.Immutable
    interface SoakingDuration {
        long sampleMinimum();

        int count();

        static SoakingDuration create(Duration duration) {
            return ImmutableSoakingDuration.builder()
                    .sampleMinimum(duration.toNanos())
                    .count(1)
                    .build();
        }

        default SoakingDuration updateDuration(Duration newDuration) {
            return ImmutableSoakingDuration.builder()
                    .count(count() + 1)
                    .sampleMinimum(Math.min(sampleMinimum(), newDuration.toNanos()))
                    .build();
        }
    }
}
