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

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(as = ImmutableTimeLockRuntimeConfig.class)
@JsonDeserialize(as = ImmutableTimeLockRuntimeConfig.class)
@Value.Immutable
public abstract class TimeLockRuntimeConfig {
    @Value.Default
    public ServerListConfig serversList() {
        return ImmutableServerListConfig.builder().build();
    }

    public ServerListConfig toNamespacedServerList(String client) {
        return ServerListConfigs.namespaceUris(serversList(), client);
    }
}
