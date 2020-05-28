/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.watch;

import java.util.Comparator;
import java.util.UUID;

import org.immutables.value.Value;

import com.palantir.logsafe.Preconditions;

@Value.Immutable
public interface IdentifiedVersion {
    @Value.Parameter
    UUID id();
    @Value.Parameter
    long version();

    static IdentifiedVersion of(UUID id, long version) {
        return ImmutableIdentifiedVersion.of(id, version);
    }

    static Comparator<IdentifiedVersion> comparator() {
        return (version1, version2) -> {
            Preconditions.checkArgument(version1.id().equals(version2.id()));
            return Long.compare(version1.version(), version2.version());
        };
    }
}
