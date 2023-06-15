/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.buggify.api;

import java.security.SecureRandom;

public interface NativeSamplingSecureRandomFactory {
    /**
     * Creates an instance of {@link SecureRandom} that, as far as is supported by the system, produces both seed and
     * random numbers from an external source.
     * <p>
     * The motivation for this is for running tests in an environment (such as that provided by Antithesis) where
     * system-provided randomness is deterministic, and can be dynamically controlled by the fuzzer to explore the
     * state space of the system more efficiently. We thus want to ensure that the random number generator we use
     * makes queries to the system-provided randomness source not just for seeding, but also on every generation
     * of a random number.
     */
    SecureRandom create();
}
