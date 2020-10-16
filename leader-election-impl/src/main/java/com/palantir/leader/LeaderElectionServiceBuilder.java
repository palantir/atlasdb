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

package com.palantir.leader;

import com.palantir.logsafe.Preconditions;
import com.palantir.paxos.CoalescingPaxosLatestRoundVerifier;
import com.palantir.paxos.LeaderPinger;
import com.palantir.paxos.PaxosAcceptorNetworkClient;
import com.palantir.paxos.PaxosLatestRoundVerifier;
import com.palantir.paxos.PaxosLatestRoundVerifierImpl;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerNetworkClient;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosProposerImpl;
import java.time.Duration;
import java.util.UUID;
import java.util.function.UnaryOperator;
import javax.annotation.Nullable;

@SuppressWarnings({"HiddenField", "OverloadMethodsDeclarationOrder"})
public final class LeaderElectionServiceBuilder {

    @Nullable
    private PaxosAcceptorNetworkClient acceptorClient;

    @Nullable
    private PaxosLearnerNetworkClient learnerClient;

    @Nullable
    private PaxosLearner knowledge;

    @Nullable
    private LeaderPinger leaderPinger;

    @Nullable
    private PaxosLeaderElectionEventRecorder eventRecorder = PaxosLeaderElectionEventRecorder.NO_OP;

    @Nullable
    private Duration pingRate;

    @Nullable
    private Duration randomWaitBeforeProposingLeadership;

    @Nullable
    private Duration leaderAddressCacheTtl;

    @Nullable
    private UUID leaderUuid;

    @Nullable
    private PaxosLatestRoundVerifier latestRoundVerifier;

    private UnaryOperator<PaxosProposer> proposerDecorator = paxosProposer -> paxosProposer;

    public LeaderElectionServiceBuilder acceptorClient(PaxosAcceptorNetworkClient acceptorClient) {
        this.acceptorClient = Preconditions.checkNotNull(acceptorClient, "acceptorClient cannot be null");
        return this;
    }

    public LeaderElectionServiceBuilder learnerClient(PaxosLearnerNetworkClient learnerClient) {
        this.learnerClient = Preconditions.checkNotNull(learnerClient, "learnerClient cannot be null");
        return this;
    }

    public LeaderElectionServiceBuilder knowledge(PaxosLearner knowledge) {
        this.knowledge = Preconditions.checkNotNull(knowledge, "knowledge cannot be null");
        return this;
    }

    public LeaderElectionServiceBuilder leaderPinger(LeaderPinger leaderPinger) {
        this.leaderPinger = Preconditions.checkNotNull(leaderPinger, "leaderPinger cannot be null");
        return this;
    }

    public LeaderElectionServiceBuilder eventRecorder(PaxosLeaderElectionEventRecorder eventRecorder) {
        this.eventRecorder = Preconditions.checkNotNull(eventRecorder, "eventRecorder cannot be null");
        return this;
    }

    public LeaderElectionServiceBuilder pingRate(Duration pingRate) {
        Preconditions.checkNotNull(pingRate, "pingRate cannot be null");
        Preconditions.checkArgument(!pingRate.isNegative(), "pingRate must be positive");
        this.pingRate = pingRate;
        return this;
    }

    public LeaderElectionServiceBuilder randomWaitBeforeProposingLeadership(
            Duration randomWaitBeforeProposingLeadership) {
        Preconditions.checkNotNull(
                randomWaitBeforeProposingLeadership, "randomWaitBeforeProposingLeadership cannot be null");
        Preconditions.checkArgument(
                !randomWaitBeforeProposingLeadership.isNegative(),
                "randomWaitBeforeProposingLeadership must be positive");
        this.randomWaitBeforeProposingLeadership = randomWaitBeforeProposingLeadership;
        return this;
    }

    public LeaderElectionServiceBuilder leaderAddressCacheTtl(Duration leaderAddressCacheTtl) {
        Preconditions.checkNotNull(leaderAddressCacheTtl, "leaderAddressCacheTtl cannot be null");
        Preconditions.checkArgument(!leaderAddressCacheTtl.isNegative(), "leaderAddressCacheTtl must be positive");
        this.leaderAddressCacheTtl = leaderAddressCacheTtl;
        return this;
    }

    public LeaderElectionServiceBuilder leaderUuid(UUID leaderUuid) {
        this.leaderUuid = Preconditions.checkNotNull(leaderUuid, "leaderUuid cannot be null");
        return this;
    }

    public LeaderElectionServiceBuilder decorateProposer(UnaryOperator<PaxosProposer> proposerDecorator) {
        this.proposerDecorator = Preconditions.checkNotNull(proposerDecorator, "proposerDecorator cannot be null");
        return this;
    }

    public LeaderElectionServiceBuilder latestRoundVerifier(PaxosLatestRoundVerifier latestRoundVerifier) {
        this.latestRoundVerifier =
                Preconditions.checkNotNull(latestRoundVerifier, "latestRoundVerifier cannot be null");
        return this;
    }

    public LeaderElectionService build() {
        return new PaxosLeaderElectionService(
                proposerDecorator.apply(buildProposer()),
                knowledge(),
                leaderPinger(),
                latestRoundVerifier(),
                learnerClient(),
                pingRate(),
                randomWaitBeforeProposingLeadership(),
                leaderAddressCacheTtl(),
                eventRecorder());
    }

    private PaxosProposer buildProposer() {
        return PaxosProposerImpl.newProposer(acceptorClient(), learnerClient(), leaderUuid());
    }

    private PaxosAcceptorNetworkClient acceptorClient() {
        return Preconditions.checkNotNull(acceptorClient, "acceptorClient not set");
    }

    private PaxosLearnerNetworkClient learnerClient() {
        return Preconditions.checkNotNull(learnerClient, "learnerClient not set");
    }

    private PaxosLatestRoundVerifier latestRoundVerifier() {
        if (latestRoundVerifier == null) {
            return new CoalescingPaxosLatestRoundVerifier(new PaxosLatestRoundVerifierImpl(acceptorClient()));
        } else {
            return latestRoundVerifier;
        }
    }

    private PaxosLearner knowledge() {
        return Preconditions.checkNotNull(knowledge, "knowledge not set");
    }

    private LeaderPinger leaderPinger() {
        return Preconditions.checkNotNull(leaderPinger, "leaderPinger not set");
    }

    private PaxosLeaderElectionEventRecorder eventRecorder() {
        return Preconditions.checkNotNull(eventRecorder, "eventRecorder not set");
    }

    private Duration pingRate() {
        return Preconditions.checkNotNull(pingRate, "pingRate not set");
    }

    private Duration randomWaitBeforeProposingLeadership() {
        return Preconditions.checkNotNull(
                randomWaitBeforeProposingLeadership, "randomWaitBeforeProposingLeadership not set");
    }

    private Duration leaderAddressCacheTtl() {
        return Preconditions.checkNotNull(leaderAddressCacheTtl, "leaderAddressCacheTtl not set");
    }

    private UUID leaderUuid() {
        return Preconditions.checkNotNull(leaderUuid, "leaderUuid not set");
    }
}
