/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.schema;

import java.util.Map;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.protos.generated.SchemaMetadataPersistence.CleanupRequirement;

public final class CleanupRequirementTracker {
    private final Map<String, CleanupRequirement> requirements = Maps.newHashMap();

    public CleanupRequirement getRequirementForTable(String tableName) {
        return requirements.getOrDefault(tableName, CleanupRequirement.NOT_NEEDED);
    }

    public void specifyRequirement(String tableName, CleanupRequirement requirement) {
        requirements.compute(tableName,
                (unused, knownRequirement) -> {
                    CleanupRequirement nonNullRequirement =
                            knownRequirement == null ? CleanupRequirement.NOT_NEEDED : knownRequirement;
                    return requirement.compareTo(nonNullRequirement) >= 0 ? requirement : nonNullRequirement;
                });
    }
}
