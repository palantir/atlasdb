/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.config;

import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Optional;
import java.util.function.Supplier;
import org.immutables.value.Value;

/**
 * Additional parameters for clients to specify when connecting to remote services.
 */
@Value.Immutable
public interface AuxiliaryRemotingParameters {
    UserAgent userAgent();

    boolean shouldLimitPayload();

    /**
     * Whether clients should retry in the event of connection failures.
     * This value may be ignored and assumed to be true for proxies that implement failover.
     */
    Optional<Boolean> shouldRetry();

    @Value.Lazy
    default boolean definitiveRetryIndication() {
        return shouldRetry().orElseThrow(() -> new SafeIllegalStateException("Attempted to determine"
                    + " definitively if we should retry, but this was unknown."));
    }

    /**
     * Whether this client should support operations that may potentially require a longer connection timeout,
     * e.g. because they are dependent on external resources like a lock.
     *
     * This is set to true by default, to support legacy behaviour.
     */
    @Value.Default
    default boolean shouldUseExtendedTimeout() {
        return true;
    }

    /**
     * User-configurable options for remoting.
     */
    @Value.Default
    default Supplier<RemotingClientConfig> remotingClientConfig() {
        return () -> RemotingClientConfigs.DEFAULT;
    }

    static ImmutableAuxiliaryRemotingParameters.Builder builder() {
        return ImmutableAuxiliaryRemotingParameters.builder();
    }
}
