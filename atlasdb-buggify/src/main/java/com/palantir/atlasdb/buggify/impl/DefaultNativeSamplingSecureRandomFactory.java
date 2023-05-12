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

package com.palantir.atlasdb.buggify.impl;

import com.palantir.atlasdb.buggify.api.NativeSamplingSecureRandomFactory;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.security.SecureRandom;

public enum DefaultNativeSamplingSecureRandomFactory implements NativeSamplingSecureRandomFactory {
    INSTANCE;

    private static final SafeLogger log = SafeLoggerFactory.get(DefaultNativeSamplingSecureRandomFactory.class);

    @Override
    public SecureRandom create() {
        try {
            // The NativePRNG algorithm reads from /dev/random or /dev/urandom for generating both seed and random
            // numbers, which satisfies the requirements of the interface (and both of these files are fuzzer-controlled
            // in Antithesis). This string is a reserved identifier for the algorithm, following
            // https://docs.oracle.com/en/java/javase/17/docs/specs/security/standard-names.html#securerandom-number-generation-algorithms
            return SecureRandom.getInstance("NativePRNG");
        } catch (Exception e) {
            log.warn(
                    "Failed to initialize fully native secure random instance, letting the Java runtime select for us",
                    e);
            return new SecureRandom();
        }
    }
}
