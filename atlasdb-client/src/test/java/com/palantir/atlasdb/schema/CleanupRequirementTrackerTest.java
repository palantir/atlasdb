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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.palantir.atlasdb.protos.generated.SchemaMetadataPersistence.CleanupRequirement;

public class CleanupRequirementTrackerTest {
    private final CleanupRequirementTracker CLEANUP_REQUIREMENTS = new CleanupRequirementTracker();

    @Test
    public void unspecifiedTableHasNoCleanupRequirement() {
        assertThat(CLEANUP_REQUIREMENTS.getRequirementForTable("random")).isEqualTo(CleanupRequirement.NOT_NEEDED);
    }

    @Test
    public void tableExplicitlySpecifiedAsNotHavingCleanupRequirementHasNoCleanupRequirement() {
        String safeTable = "safe";
        CLEANUP_REQUIREMENTS.specifyRequirement(safeTable, CleanupRequirement.NOT_NEEDED);
        assertThat(CLEANUP_REQUIREMENTS.getRequirementForTable(safeTable)).isEqualTo(CleanupRequirement.NOT_NEEDED);
    }

    @Test
    public void selectsStrictestCleanupRequirementIfSpecifiedMultipleTimes() {
        String unsafeTable = "unsafe";
        CLEANUP_REQUIREMENTS.specifyRequirement(unsafeTable, CleanupRequirement.NOT_NEEDED);
        CLEANUP_REQUIREMENTS.specifyRequirement(unsafeTable, CleanupRequirement.STREAM_STORE);
        CLEANUP_REQUIREMENTS.specifyRequirement(unsafeTable, CleanupRequirement.ARBITRARY_SYNC);
        CLEANUP_REQUIREMENTS.specifyRequirement(unsafeTable, CleanupRequirement.ARBITRARY_ASYNC);
        assertThat(CLEANUP_REQUIREMENTS.getRequirementForTable(unsafeTable))
                .isEqualTo(CleanupRequirement.ARBITRARY_SYNC);
    }

    @Test
    public void multipleSpecificationsOfSameRequirementDoesNotPromoteTheRequirement() {
        String asyncTable = "async";
        CLEANUP_REQUIREMENTS.specifyRequirement(asyncTable, CleanupRequirement.ARBITRARY_ASYNC);
        CLEANUP_REQUIREMENTS.specifyRequirement(asyncTable, CleanupRequirement.ARBITRARY_ASYNC);
        CLEANUP_REQUIREMENTS.specifyRequirement(asyncTable, CleanupRequirement.ARBITRARY_ASYNC);
        assertThat(CLEANUP_REQUIREMENTS.getRequirementForTable(asyncTable))
                .isEqualTo(CleanupRequirement.ARBITRARY_ASYNC);
    }
}
