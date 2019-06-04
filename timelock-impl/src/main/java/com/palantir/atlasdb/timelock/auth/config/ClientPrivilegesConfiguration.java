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

package com.palantir.atlasdb.timelock.auth.config;

import java.util.List;
import java.util.Set;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.auth.api.ClientId;
import com.palantir.atlasdb.timelock.auth.api.Privileges;
import com.palantir.lock.TimelockNamespace;

@JsonSerialize(as = ImmutableClientPrivilegesConfiguration.class)
@JsonDeserialize(as = ImmutableClientPrivilegesConfiguration.class)
@JsonTypeName(ClientPrivilegesConfiguration.TYPE)
@Value.Immutable
public abstract class ClientPrivilegesConfiguration implements PrivilegesConfiguration {
    static final String TYPE = "client";

    private static final List<String> BANNED_SUFFIXES = ImmutableList.of("", " ");

    @JsonProperty("client-id")
    @Override
    public abstract ClientId clientId();

    abstract Set<TimelockNamespace> namespaces();

    @JsonProperty("namespace-suffixes")
    @Value.Default
    public Set<String> namespaceSuffixes() {
        return ImmutableSet.of();
    }

    @Value.Derived
    @Override
    public Privileges privileges() {
        return NamespacePrivileges.of(namespaces(), namespaceSuffixes());
    }

    @Value.Check
    protected void check() {
        BANNED_SUFFIXES.forEach(bannedSuffix -> Preconditions.checkArgument(!namespaceSuffixes().contains(bannedSuffix),
                    "\"%s\" can not be use as a suffix", bannedSuffix));
    }
}
