/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.timelock.config;

import java.util.Set;

import javax.validation.constraints.Size;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;

import io.atomix.catalyst.transport.Address;

@JsonSerialize(as = ImmutableClusterConfiguration.class)
@JsonDeserialize(as = ImmutableClusterConfiguration.class)
@Value.Immutable
public abstract class ClusterConfiguration {
    public abstract Address localServer();

    @Size(min = 1)
    public abstract Set<Address> servers();

    @Value.Check
    protected void check() {
        Preconditions.checkArgument(servers().contains(localServer()),
                "The localServer '%s' must be included in the server entries %s.", localServer(), servers());
    }
}
