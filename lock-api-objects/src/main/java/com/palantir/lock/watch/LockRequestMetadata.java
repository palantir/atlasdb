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

package com.palantir.lock.watch;

import com.palantir.lock.LockDescriptor;
import com.palantir.logsafe.Unsafe;
import java.util.Map;
import org.immutables.value.Value;

/**
 * Metadata attached to a lock request and to any {@link LockEvent} created as a result of that request.
 * Ideally, this class is not serialized as-is to JSON due to the repetition of {@link LockDescriptor} values
 * in {@link LockRequestMetadata#lockDescriptorToChangeMetadata()} and the parent object.
 */
@Unsafe
@Value.Immutable
public interface LockRequestMetadata {

    @Value.Parameter
    Map<LockDescriptor, ChangeMetadata> lockDescriptorToChangeMetadata();

    static LockRequestMetadata of(Map<LockDescriptor, ChangeMetadata> lockDescriptorToChangeMetadata) {
        return ImmutableLockRequestMetadata.of(lockDescriptorToChangeMetadata);
    }
}
