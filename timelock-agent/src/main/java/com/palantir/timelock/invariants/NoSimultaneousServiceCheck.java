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

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.timelock.paxos.Client;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.timelock.TimeLockStatus;
import com.palantir.timelock.paxos.HealthCheckDigest;

public final class NoSimultaneousServiceCheck {
    private static final Logger log = LoggerFactory.getLogger(NoSimultaneousServiceCheck.class);

    private static final int REQUIRED_CONSECUTIVE_VIOLATIONS_BEFORE_FAIL = 5;
    private static final Duration BACKOFF = Duration.ofMillis(1337);

    private final List<TimeLockActivityChecker> timeLockActivityCheckers;
    private final Consumer<String> failureMechanism;
    private final ExecutorService executorService;

    @VisibleForTesting
    NoSimultaneousServiceCheck(
            List<TimeLockActivityChecker> timeLockActivityCheckers,
            Consumer<String> failureMechanism,
            ExecutorService executorService) {
        this.timeLockActivityCheckers = timeLockActivityCheckers;
        this.failureMechanism = failureMechanism;
        this.executorService = executorService;
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
                executorService);
    }

    public void processHealthCheckDigest(HealthCheckDigest digest) {
        Set<Client> clientsWithMultipleLeaders = digest.statusesToClient().get(TimeLockStatus.MULTIPLE_LEADERS);
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
        for (int attempt = 1; attempt <= REQUIRED_CONSECUTIVE_VIOLATIONS_BEFORE_FAIL; attempt++) {
            long numberOfNodesServingTimestamps = timeLockActivityCheckers.stream()
                    .map(timeLockActivityChecker ->
                            timeLockActivityChecker.isThisNodeActivelyServingTimestampsForClient(client.value()))
                    .filter(x -> x)
                    .count();
            if (numberOfNodesServingTimestamps <= 1) {
                // Accept 0: the cluster being in such a bad state is not a terminal condition, could just be a
                // network partition or legitimate no-quorum situation. No reason to kill the server then.
                log.info("We don't think services were simultaneously serving timestamps for client {}",
                        SafeArg.of("client", client));
                return;
            }

            if (attempt < REQUIRED_CONSECUTIVE_VIOLATIONS_BEFORE_FAIL) {
                log.info("We observed on attempt {} of {} that multiple services were serving timestamps. We'll try"
                                + " again in {} ms to see if this remains the case.",
                        SafeArg.of("attemptNumber", attempt),
                        SafeArg.of("maximumAttempts", REQUIRED_CONSECUTIVE_VIOLATIONS_BEFORE_FAIL),
                        SafeArg.of("backoffMillis", BACKOFF.toMillis()));
                Uninterruptibles.sleepUninterruptibly(BACKOFF.toMillis(), TimeUnit.MILLISECONDS);
            } else {
                failureMechanism.accept(client.value());
            }
        }
    }
}
