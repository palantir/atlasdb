/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.asts.bucketingthings;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

@JsonSerialize(as = ImmutableBucketStateAndIdentifier.class)
@JsonDeserialize(as = ImmutableBucketStateAndIdentifier.class)
@Value.Immutable
public interface BucketStateAndIdentifier {
    @Value.Parameter
    long bucketIdentifier();

    @Value.Parameter
    BucketAssignerState state();

    static BucketStateAndIdentifier of(long bucketIdentifier, BucketAssignerState state) {
        return ImmutableBucketStateAndIdentifier.of(bucketIdentifier, state);
    }
}
