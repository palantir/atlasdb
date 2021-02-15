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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.timelock.auth.api.ClientId;
import com.palantir.atlasdb.timelock.auth.api.Privileges;
import org.immutables.value.Value;

@JsonSerialize(as = ImmutableAdminPrivilegesConfiguration.class)
@JsonDeserialize(as = ImmutableAdminPrivilegesConfiguration.class)
@JsonTypeName(AdminPrivilegesConfiguration.TYPE)
@Value.Immutable
public abstract class AdminPrivilegesConfiguration implements PrivilegesConfiguration {
    static final String TYPE = "admin";

    @JsonProperty("client-id")
    @Override
    public abstract ClientId clientId();

    @Value.Derived
    @Override
    public Privileges privileges() {
        return Privileges.ADMIN;
    }
}
