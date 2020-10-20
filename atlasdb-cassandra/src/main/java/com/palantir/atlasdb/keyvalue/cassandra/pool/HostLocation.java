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

package com.palantir.atlasdb.keyvalue.cassandra.pool;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.conjure.java.client.config.ImmutablesStyle;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableHostLocation.class)
@JsonSerialize(as = ImmutableHostLocation.class)
@ImmutablesStyle
@Value.Immutable
public interface HostLocation {

    @Value.Parameter
    String datacenter();

    @Value.Parameter
    String rack();

    static HostLocation of(String datacenter, String rack) {
        return ImmutableHostLocation.of(datacenter, rack);
    }

    default boolean isProbablySameRackAs(String datacenter, String rack) {
        return rack().equals(rack) && datacenter.startsWith(datacenter());
    }
}
