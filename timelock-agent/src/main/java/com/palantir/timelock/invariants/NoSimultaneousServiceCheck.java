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

package com.palantir.timelock.invariants;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.Client;
import com.palantir.timelock.TimeLockStatus;
import com.palantir.timelock.paxos.HealthCheckDigest;
import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class NoSimultaneousServiceCheck {
    private static final Logger log = LoggerFactory.getLogger(NoSimultaneousServiceCheck.class);

    private static final int REQUIRED_ATTEMPTS_BEFORE_GIVING_UP = 5;

    private final List<TimeLockActivityChecker> timeLockActivityCheckers;
    private final Consumer<String> failureMechanism;
    private final ExecutorService executorService;
    private final Duration backoff;

    @VisibleForTesting
    NoSimultaneousServiceCheck(
            List<TimeLockActivityChecker> timeLockActivityCheckers,
            Consumer<String> failureMechanism,
            ExecutorService executorService,
            Duration backoff) {
        this.timeLockActivityCheckers = timeLockActivityCheckers;
        this.failureMechanism = failureMechanism;
        this.executorService = executorService;
        this.backoff = backoff;
    }

    public static NoSimultaneousServiceCheck create(List<TimeLockActivityChecker> timeLockActivityCheckers) {
        ExecutorService executorService = PTExecutors.newSingleThreadExecutor(
                PTExecutors.newNamedThreadFactory(false));
        return new NoSimultaneousServiceCheck(timeLockActivityCheckers,
                client -> {
                    // TODO (jkong): Gather confidence and then change to ServerKiller, so that we ACTUALLY shoot
                    // ourselves in the head.
                    log.error("We observed that multiple services were consistently serving timestamps, for the"
                            + " client {}. This is potentially indicative of SEVERE DATA CORRUPTION, and should"
                            + " never happen in a correct TimeLock implementation. If you see this message, please"
                            + " check the frequency of leader elections on your stack: if they are very frequent,"
                            + " consider increasing the leader election timeout. Otherwise, please contact support -"
                            + " your stack may have been compromised",
                            SafeArg.of("client", client));
                },
                executorService,
                Duration.ofMillis(1337));
    }

    public void processHealthCheckDigest(HealthCheckDigest digest) {
        Set<Client> clientsWithMultipleLeaders = digest.statusesToClient().get(TimeLockStatus.MULTIPLE_LEADERS);
        if (clientsWithMultipleLeaders.isEmpty()) {
            return;
        }

        log.info("Clients {} appear to have multiple leaders based on the leader ping health check. Scheduling"
                + " checks on these specific clients now.", SafeArg.of("clients", clientsWithMultipleLeaders));
        clientsWithMultipleLeaders.forEach(this::scheduleCheckOnSpecificClient);
    }

    private void scheduleCheckOnSpecificClient(Client client) {
        executorService.submit(() -> {
            try {
                performCheckOnSpecificClientUnsafe(client);
            } catch (Exception e) {
                log.info("No-simultaneous service check failed, suppressing exception to allow future checks", e);
            }
        });
    }

    private void performCheckOnSpecificClientUnsafe(Client client) {
        // Only fail on repeated violations, since it is possible for there to be a leader election between checks that
        // could legitimately cause false positives if we failed after one such issue. However, given the number of
        // checks it is unlikely that *that* many elections would occur.
        long timestampBound = Long.MIN_VALUE;

        for (int attempt = 1; attempt <= REQUIRED_ATTEMPTS_BEFORE_GIVING_UP; attempt++) {
            List<Long> timestamps = timeLockActivityCheckers.stream()
                    .map(timeLockActivityChecker ->
                            timeLockActivityChecker.getFreshTimestampFromNodeForClient(client.value()))
                    .filter(OptionalLong::isPresent)
                    .map(OptionalLong::getAsLong)
                    .collect(Collectors.toList());
            if (timestamps.size() <= 1) {
                // Accept 0: the cluster being in such a bad state is not a terminal condition, could just be a
                // network partition or legitimate no-quorum situation. No reason to kill the server then.
                log.info("We don't think services were simultaneously serving timestamps for client {}",
                        SafeArg.of("client", client));
                return;
            }

            if (!Ordering.natural().isStrictlyOrdered(timestamps) || timestamps.get(0) <= timestampBound) {
                failureMechanism.accept(client.value());
                return;
            }

            timestampBound = timestamps.get(timestamps.size() - 1);
            log.info("We observed on attempt that multiple services were serving timestamps, but the timestamps were"
                            + " served in an increasing order. We'll try again in {} ms to see if this remains the"
                            + " case.",
                    SafeArg.of("backoffMillis", backoff.toMillis()));
            Uninterruptibles.sleepUninterruptibly(backoff.toMillis(), TimeUnit.MILLISECONDS);
        }
        log.warn("We observed multiple services apparently serving timestamps simultaneously, but the timestamps"
                + " were consistently served in increasing order.");
    }
}
