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

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import io.atomix.copycat.server.storage.StorageLevel;

@JsonSerialize(as = ImmutableAtomixConfiguration.class)
@JsonDeserialize(as = ImmutableAtomixConfiguration.class)
@Value.Immutable
public abstract class AtomixConfiguration {
    public static final AtomixConfiguration DEFAULT = ImmutableAtomixConfiguration.builder().build();

    @Value.Default
    public StorageLevel storageLevel() {
        return StorageLevel.DISK;
    }

    @Value.Default
    public String storageDirectory() {
        return "var/data/atomix";
    }
}
