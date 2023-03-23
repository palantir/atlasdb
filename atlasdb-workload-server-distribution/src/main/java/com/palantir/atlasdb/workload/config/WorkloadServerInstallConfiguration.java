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

package com.palantir.atlasdb.workload.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.workload.workflow.SingleRowTwoCellsWorkflowConfiguration;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableWorkloadServerInstallConfiguration.class)
@JsonSerialize(as = ImmutableWorkloadServerInstallConfiguration.class)
@Value.Immutable
public interface WorkloadServerInstallConfiguration {
    @JsonProperty("atlas")
    AtlasDbConfig atlas();

    @JsonProperty("single-row-two-cells-config")
    SingleRowTwoCellsWorkflowConfiguration singleRowTwoCellsConfig();

    @JsonProperty("exit-after-running")
    boolean exitAfterRunning();
}
