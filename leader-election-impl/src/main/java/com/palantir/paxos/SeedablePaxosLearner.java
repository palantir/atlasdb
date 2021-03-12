/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SeedablePaxosLearner implements PaxosLearner {
    private static final Logger log = LoggerFactory.getLogger(SeedablePaxosLearner.class);

    private final AtomicReference<PaxosLearner> delegateReference = new AtomicReference<>();

    public void supplyDelegate(PaxosLearner delegate) {
        if (!delegateReference.compareAndSet(null, delegate)) {
            log.warn(
                    "Attempted to set the delegate of a seedable Paxos learner multiple times! This is indicative "
                            + "of a bug in the atlasdb or timelock code. We'll just use the first delegate.",
                    new RuntimeException("I exist to show you the stack trace"));
        }
    }

    @Override
    public void learn(long seq, PaxosValue val) {}

    @Override
    public Optional<PaxosValue> getLearnedValue(long seq) {
        return Optional.empty();
    }

    @Override
    public Optional<PaxosValue> getGreatestLearnedValue() {
        return Optional.empty();
    }

    @Override
    public Collection<PaxosValue> getLearnedValuesSince(long seq) {
        return null;
    }
}
