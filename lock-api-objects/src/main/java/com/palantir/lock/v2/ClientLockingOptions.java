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

package com.palantir.lock.v2;

import java.time.Duration;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Options for, on the client-side, how AtlasDB lock requests should be processed. This class should not be visible
 * to TimeLock servers, and must not be leaked to other services.
 */
@Value.Immutable
public interface ClientLockingOptions {
    Runnable NO_OP_CALLBACK = () -> {};

    /**
     * After {@link ClientLockingOptions#maximumLockTenure()} has passed since the client acquired the lock, we will
     * not try to refresh the lock any longer. This is infinite by default.
     */
    Optional<Duration> maximumLockTenure();

    /**
     * Invoked if the lock tenure expired on the client. Must be cheap to execute: in particular, explicitly unlocking
     * the lock or making any other RPCs is not allowed. These callbacks may be executed serially for each lock that
     * has expired, and may block general operation of the lock server, if they are too expensive.
     */
    @Value.Default
    default Runnable tenureExpirationCallback() {
        return NO_OP_CALLBACK;
    }

    static ClientLockingOptions getDefault() {
        return builder().build();
    }

    static ImmutableClientLockingOptions.Builder builder() {
        return ImmutableClientLockingOptions.builder();
    }
}
