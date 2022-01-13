/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.management;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosRoundFailureException;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Consumer;

public class PaxosDisabledNamespacesStore implements DisabledNamespacesStore {
    private static final SafeLogger log = SafeLoggerFactory.get(PaxosDisabledNamespacesStore.class);

    private final PaxosProposer proposer;

    // surely these are used somewhere?
    //    private final PaxosLearner knowledge;
    //
    //    private final PaxosAcceptorNetworkClient acceptorNetworkClient;
    //    private final PaxosLearnerNetworkClient learnerClient;
    private final long maximumWaitBeforeProposalMs;

    private final LocalDisabledNamespacesStore localStore;

    public PaxosDisabledNamespacesStore(
            PaxosProposer proposer, long maximumWaitBeforeProposalMs, LocalDisabledNamespacesStore localStore) {
        this.proposer = proposer;
        this.maximumWaitBeforeProposalMs = maximumWaitBeforeProposalMs;
        this.localStore = localStore;
    }

    @Override
    public void disable(Set<String> namespaces) {
        modifyState(namespaces, localStore::disable);
    }

    @Override
    public void disable(String namespace) {
        disable(ImmutableSet.of(namespace));
    }

    @Override
    public void reEnable(Set<String> namespaces) {
        modifyState(namespaces, localStore::reEnable);
    }

    @Override
    public void reEnable(String namespace) {
        reEnable(ImmutableSet.of(namespace));
    }

    // TODO(gs): better name
    private void modifyState(Set<String> namespaces, Consumer<Set<String>> localModifier) {
        long newSeq = PaxosAcceptor.NO_LOG_ENTRY + 1;
        while (true) {
            try {
                byte[] proposalValue = PtBytes.toBytes(namespaces.toString());
                byte[] acceptedValue = proposer.propose(newSeq, proposalValue);
                Preconditions.checkNotNull(
                        acceptedValue,
                        "Proposed value can't be null, but was in sequence",
                        SafeArg.of("seqNum", newSeq),
                        SafeArg.of("expectedValue", namespaces));
                Preconditions.checkState(
                        Arrays.equals(proposalValue, acceptedValue),
                        "Value accepted was different from that proposed!");

                localModifier.accept(namespaces);

                return;
            } catch (PaxosRoundFailureException e) {
                waitForRandomBackoff(e, this::wait);
            }
        }
    }

    // TODO(gs): duplicated from PaxosTimestampBoundStore (probably exists in other places?)
    /**
     * Executes a backoff action which is given a random amount of time to wait in milliseconds. This is used in Paxos
     * to resolve multiple concurrent proposals. Users are allowed to specify their own backoff action,
     * to handle cases where users hold or do not hold monitor locks, for instance.
     *
     * @param paxosException the PaxosRoundFailureException that caused us to wait
     * @param backoffAction the action to take (which consumes the time, in milliseconds, to wait for)
     */
    private void waitForRandomBackoff(
            PaxosRoundFailureException paxosException, PaxosDisabledNamespacesStore.BackoffAction backoffAction) {
        long backoffTime = getRandomBackoffTime();
        log.info(
                "Paxos proposal couldn't complete, because we could not connect to a quorum of nodes. We"
                        + " will retry in {} ms.",
                SafeArg.of("backoffTime", backoffTime),
                paxosException);
        try {
            backoffAction.backoff(backoffTime);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Generates a random amount of time to wait for, in milliseconds.
     * This typically depends on the configuration of the Paxos algorithm; currently, we have implemented
     * this as U(1, k) where k is the maximum wait before proposal in the Paxos configuration.
     *
     * @return the amount of time to wait for, in milliseconds
     */
    private long getRandomBackoffTime() {
        return (long) (maximumWaitBeforeProposalMs * Math.random() + 1);
    }

    private interface BackoffAction {
        void backoff(long backoffTime) throws InterruptedException;
    }
}
