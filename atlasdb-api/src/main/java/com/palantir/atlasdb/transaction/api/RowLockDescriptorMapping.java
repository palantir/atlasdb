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

package com.palantir.atlasdb.transaction.api;

import com.palantir.common.annotations.ImmutablesStyles.PackageVisibleImmutablesStyle;
import com.palantir.lock.LockDescriptor;
import com.palantir.logsafe.Unsafe;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

@Unsafe
@Value.Immutable
@PackageVisibleImmutablesStyle
public abstract class RowLockDescriptorMapping {
    abstract Map<LockDescriptor, RowReference> mapping();

    public Optional<RowReference> rowReferenceForDescriptor(LockDescriptor lockDescriptor) {
        return Optional.ofNullable(mapping().get(lockDescriptor));
    }

    public static RowLockDescriptorMapping of(Map<LockDescriptor, RowReference> mapping) {
        return ImmutableRowLockDescriptorMapping.builder().mapping(mapping).build();
    }
}
