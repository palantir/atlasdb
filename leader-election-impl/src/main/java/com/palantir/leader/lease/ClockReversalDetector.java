/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.leader.lease;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Suppliers;

final class ClockReversalDetector {
    private static final Logger log = LoggerFactory.getLogger(ClockReversalDetector.class);
    private static final Supplier<Boolean> memoizedValue =
            Suppliers.memoizeWithExpiration(ClockReversalDetector::update, 1, TimeUnit.SECONDS);

    private static NanoTime lastMeasured = NanoTime.now();
    private static boolean neverFailed = true;

    private static boolean update() {
        NanoTime now = NanoTime.now();
        if (now.isBefore(lastMeasured)) {
            log.error("We saw our system clock go backwards, so we are removing all lease-based optimisations");
            neverFailed = false;
        }
        lastMeasured = now;
        return neverFailed;
    }

    static boolean canUseLeaseBasedOptimizations() {
        return memoizedValue.get();
    }
}
