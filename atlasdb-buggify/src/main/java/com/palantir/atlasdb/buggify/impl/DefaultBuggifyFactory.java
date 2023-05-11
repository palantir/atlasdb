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

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.buggify.api.Buggify;
import com.palantir.atlasdb.buggify.api.BuggifyFactory;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.security.SecureRandom;
import java.util.function.DoubleSupplier;

public final class DefaultBuggifyFactory implements BuggifyFactory {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultBuggifyFactory.class);
    private static final SecureRandom SECURE_RANDOM;
    public static final BuggifyFactory INSTANCE = new DefaultBuggifyFactory();

    static {
        SecureRandom randomInstance;
        try {
            // See https://docs.oracle.com/en/java/javase/17/docs/specs/security/standard-names.html#securerandom-number-generation-algorithms
            randomInstance = SecureRandom.getInstance("NativePRNG");
        } catch (Exception e) {
            log.warn("Failed to initialize fully native secure random instance, letting the Java runtime select for us", e);
            randomInstance = new SecureRandom();
        }
        SECURE_RANDOM = randomInstance;
    }

    private final DoubleSupplier doubleSupplier;

    @VisibleForTesting
    DefaultBuggifyFactory(DoubleSupplier doubleSupplier) {
        this.doubleSupplier = doubleSupplier;
    }

    private DefaultBuggifyFactory() {
        this(SECURE_RANDOM::nextDouble);
    }

    @Override
    public Buggify maybe(double probability) {
        if (doubleSupplier.getAsDouble() < probability) {
            return DefaultBuggify.INSTANCE;
        }
        return NoOpBuggify.INSTANCE;
    }
}
