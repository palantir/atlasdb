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

import com.google.common.collect.Sets;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.timelock.feedback.LeaderElectionDuration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongConsumer;
import org.immutables.value.Value;
import org.immutables.value.Value.Parameter;

public class LeaderElectionDurationAccumulator {
    private Set<LeadersContext> alreadyReportedLeaderElections = Sets.newConcurrentHashSet();
    private Map<LeadersContext, ModifiableSoakingDuration> currentlySoaking = new ConcurrentHashMap<>();

    private final LongConsumer consumer;
    private final int updatesToAchieveConfidence;

    /**
     * This class accumulates {@link LeaderElectionDuration}s by tracking the shortest duration observed for each pair
     * of leader ids until updatesToAchieveConfidence have been received for that pair. Once that occurs, the
     * final duration is consumed by the consumer, and further updates for the leader id pair are ignored.
     *
     * @param updatesToAchieveConfidence required number of updates to before the results are reported, must be
     *                                   greater than 1
     */
    public LeaderElectionDurationAccumulator(LongConsumer consumer, int updatesToAchieveConfidence) {
        this.consumer = consumer;
        this.updatesToAchieveConfidence = updatesToAchieveConfidence;
        Preconditions.checkArgument(updatesToAchieveConfidence > 1,
                "Number of required updates must be greater than 1.",
                SafeArg.of("updatesToAchieveConfidence", updatesToAchieveConfidence));
    }

    public void add(LeaderElectionDuration duration) {
        LeadersContext leadersContext = LeadersContext.of(duration.getOldLeader(), duration.getNewLeader());
        if (alreadyReportedLeaderElections.contains(leadersContext)) {
            return;
        }
        currentlySoaking.compute(
                leadersContext,
                (context, previous) -> increaseConfidence(context, previous, duration.getDuration().longValue()));
    }

    private ModifiableSoakingDuration increaseConfidence(
            LeadersContext context, ModifiableSoakingDuration accumulatedSoakingDuration, long duration) {
        if (alreadyReportedLeaderElections.contains(context)) {
            return null;
        }
        if (accumulatedSoakingDuration == null) {
            return ModifiableSoakingDuration.create(duration, 1);
        }
        long accumulatedDuration = accumulatedSoakingDuration.value();
        if (accumulatedDuration > duration) {
            accumulatedSoakingDuration.setValue(duration);
        }
        int currentCount = accumulatedSoakingDuration.count() + 1;
        if (currentCount >= updatesToAchieveConfidence) {
            alreadyReportedLeaderElections.add(context);
            consumer.accept(accumulatedSoakingDuration.value());
            return null;
        }
        accumulatedSoakingDuration.setCount(currentCount);
        return accumulatedSoakingDuration;
    }

    @Value.Immutable
    interface LeadersContext {
        @Parameter
        UUID previous();

        @Parameter
        UUID next();

        static LeadersContext of(UUID previous, UUID next) {
            return ImmutableLeadersContext.of(previous, next);
        }
    }

    @Value.Modifiable
    interface SoakingDuration {
        @Parameter
        long value();

        @Parameter
        int count();
    }
}
