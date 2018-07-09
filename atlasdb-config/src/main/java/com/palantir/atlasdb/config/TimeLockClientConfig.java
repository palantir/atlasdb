/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.config;

import java.util.Optional;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;

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

    @JsonIgnore
    @Value.Lazy
    public String getClientOrThrow() {
        return client().orElseThrow(() -> new IllegalStateException(
                "Tried to read a client from a TimeLockClientConfig, but it hadn't been initialised."));
    }

    @Value.Check
    protected final void check() {
        Preconditions.checkArgument(!client().isPresent() || !client().get().isEmpty(),
                "Timelock client string cannot be empty");
    }
}
