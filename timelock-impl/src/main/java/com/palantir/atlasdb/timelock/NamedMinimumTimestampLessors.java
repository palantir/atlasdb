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

package com.palantir.atlasdb.timelock;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.api.NamedMinimumTimestampLeaseIdentifier;
import com.palantir.atlasdb.timelock.lock.MinimumTimestampLessor;
import com.palantir.atlasdb.timelock.lock.MinimumTimestampLessorImpl;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.Set;

public final class NamedMinimumTimestampLessors {
    private static final Set<String> PERMITTED_NAMES = ImmutableSet.of("commitImmutable");

    private final LoadingCache<NamedMinimumTimestampLeaseIdentifier, MinimumTimestampLessor> lessors =
            Caffeine.newBuilder().build(NamedMinimumTimestampLessors::loadLessor);

    public MinimumTimestampLessor getLessor(NamedMinimumTimestampLeaseIdentifier name) {
        return lessors.get(name);
    }

    private static MinimumTimestampLessor loadLessor(NamedMinimumTimestampLeaseIdentifier name) {
        if (!PERMITTED_NAMES.contains(name.get())) {
            throw new SafeIllegalArgumentException(
                    "Unexpected name for minimum lease manager", SafeArg.of("name", name));
        }
        return new MinimumTimestampLessorImpl();
    }
}
