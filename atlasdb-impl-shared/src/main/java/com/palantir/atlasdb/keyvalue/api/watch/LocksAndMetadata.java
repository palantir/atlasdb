/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api.watch;

import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.LockRequestMetadata;
import com.palantir.logsafe.Unsafe;
import java.util.Optional;
import java.util.Set;
import org.immutables.value.Value;

@Unsafe
@Value.Immutable
public interface LocksAndMetadata {
    LocksAndMetadata EMPTY = LocksAndMetadata.of(Set.of(), Optional.empty());

    @Value.Parameter
    Set<LockDescriptor> lockDescriptors();

    @Value.Parameter
    Optional<LockRequestMetadata> metadata();

    static LocksAndMetadata of(Set<LockDescriptor> lockDescriptors, Optional<LockRequestMetadata> metadata) {
        return ImmutableLocksAndMetadata.of(lockDescriptors, metadata);
    }
}
