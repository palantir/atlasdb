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

package com.palantir.atlasdb.workload.workflow;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableRandomWorkflowConfiguration.class)
@JsonDeserialize(as = ImmutableRandomWorkflowConfiguration.class)
@JsonTypeName(MultipleBusyCellWorkflowConfiguration.TYPE)
public interface MultipleBusyCellWorkflowConfiguration extends WorkflowConfiguration {
    String TYPE = "multiple-busy-cell";

    TableConfiguration tableConfiguration();

    @Value.Default
    default int maxCells() {
        return 5;
    }

    @Value.Default
    default double deleteProbability() {
        return 0.2;
    }

    @Value.Default
    default double proportionOfIterationCountAsUpdates() {
        return 0.9;
    }

    @Value.Lazy
    default int maxUpdates() {
        return Math.toIntExact(Math.round(proportionOfIterationCountAsUpdates() * iterationCount()));
    }

    @Value.Lazy
    default int maxReads() {
        return iterationCount() - maxUpdates();
    }
}
