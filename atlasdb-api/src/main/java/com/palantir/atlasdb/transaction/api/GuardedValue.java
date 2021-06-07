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
import org.immutables.value.Value;

@Value.Immutable
@PackageVisibleImmutablesStyle
/**
 * A GuardedValue represents a value with a limited validity window.
 *
 * In the context of the AtlasDB transaction protocol, a value with {@link #guardTimestamp()} ts is guaranteed to be
 * fresh, i.e., equal to the corresponding entry in the KVS, for any transaction with a start timestamp greater than ts
 * if there were no write locks taken out between ts and start timestamp that could have modified the stored value.
 */
public interface GuardedValue {
    @Value.Parameter
    byte[] value();

    @Value.Parameter
    long guardTimestamp();

    static GuardedValue of(byte[] value, long timestamp) {
        return ImmutableGuardedValue.of(value, timestamp);
    }
}
