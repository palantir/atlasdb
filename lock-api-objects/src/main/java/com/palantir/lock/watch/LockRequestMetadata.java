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
 * Metadata that is attached to lock requests and to any {@link LockEvent} created as a result of that request.
 */
@Unsafe
@Value.Immutable
public interface LockRequestMetadata {
    Map<LockDescriptor, ChangeMetadata> lockDescriptorToChangeMetadata();
}
