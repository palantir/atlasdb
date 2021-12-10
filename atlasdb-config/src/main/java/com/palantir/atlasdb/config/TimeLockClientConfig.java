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
package com.palantir.atlasdb.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Optional;
import org.immutables.value.Value;

@JsonSerialize(as = ImmutableTimeLockClientConfig.class)
@JsonDeserialize(as = ImmutableTimeLockClientConfig.class)
@Value.Immutable
public abstract class TimeLockClientConfig {

    /**
     * Specifies the TimeLock client name.
     * @deprecated Use the AtlasDbConfig#namespace to specify it instead.
     */
    @Deprecated
    public abstract Optional<String> client();

    @Value.Default
    public boolean shouldGiveFeedbackToTimeLockServer() {
        return true;
    }

    @JsonIgnore
    @Value.Lazy
    public String getClientOrThrow() {
        return client().orElseThrow(() -> new SafeIllegalStateException(
                "Tried to read a client from a TimeLockClientConfig, but it hadn't been initialised."));
    }

    /**
     * @deprecated Please use {@link TimeLockRuntimeConfig} to specify the {@link ServerListConfig} to be used
     * for connecting to TimeLock.
     */
    @Deprecated
    @SuppressWarnings("InlineMeSuggester")
    @Value.Default
    public ServerListConfig serversList() {
        return ImmutableServerListConfig.builder().build();
    }

    public ServerListConfig toNamespacedServerList() {
        return ServerListConfigs.namespaceUris(serversList(), getClientOrThrow());
    }

    @Value.Check
    protected final void check() {
        Preconditions.checkArgument(
                !client().isPresent() || !client().get().isEmpty(), "Timelock client string cannot be empty");
    }
}
