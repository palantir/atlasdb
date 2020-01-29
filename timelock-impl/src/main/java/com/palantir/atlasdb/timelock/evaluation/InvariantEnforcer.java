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

package com.palantir.atlasdb.timelock.evaluation;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

public class InvariantEnforcer implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(InvariantEnforcer.class);

    private final ScheduledExecutorService executor = PTExecutors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("InvariantEnforcer", true));
    private final List<Invariant> invariantsToEnforce;

    public InvariantEnforcer(List<Invariant> invariantsToEnforce) {
        this.invariantsToEnforce = invariantsToEnforce;
        executor.scheduleWithFixedDelay(this::runOneIteration, 120, 30, TimeUnit.SECONDS);
    }

    private void runOneIteration() {
        try {
            List<InvariantCheckState> violatedInvariants = invariantsToEnforce.stream()
                    .map(Invariant::runOneIteration)
                    .filter(InvariantCheckState::isDefinitivelyViolated)
                    .collect(Collectors.toList());

            if (!violatedInvariants.isEmpty()) {
                log.error("Invariant enforcer has detected one or more violations, so something bad happened and we"
                                + " can't continue. In the interest of safety, we are shutting down this service now."
                                + " The violation(s) was/were: {}.",
                        SafeArg.of("violatedInvariants", violatedInvariants));
                System.exit(1);
                throw new SafeIllegalStateException("We should have exited before we got to this point",
                        SafeArg.of("violatedInvariants", violatedInvariants));
            }

            log.info("Invariant enforcer ran and did not detect any violations.");
        } catch (Throwable t) {
            log.info("Invariant enforcer suppressed an exception.", t);
        }
    }

    @Override
    public void close() throws IOException {
        executor.shutdown();
    }
}
