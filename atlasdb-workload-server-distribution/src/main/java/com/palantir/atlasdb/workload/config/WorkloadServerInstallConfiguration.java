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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.workload.workflow.RandomWorkflowConfiguration;
import com.palantir.atlasdb.workload.workflow.SingleBusyCellReadsNoTouchWorkflowConfiguration;
import com.palantir.atlasdb.workload.workflow.SingleBusyCellWorkflowConfiguration;
import com.palantir.atlasdb.workload.workflow.SingleRowTwoCellsWorkflowConfiguration;
import com.palantir.atlasdb.workload.workflow.TransientRowsWorkflowConfiguration;
import com.palantir.atlasdb.workload.workflow.bank.BankBalanceWorkflowConfiguration;
import com.palantir.atlasdb.workload.workflow.ring.RingWorkflowConfiguration;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableWorkloadServerInstallConfiguration.class)
@JsonSerialize(as = ImmutableWorkloadServerInstallConfiguration.class)
@Value.Immutable
public interface WorkloadServerInstallConfiguration {
    AtlasDbConfig atlas();

    SingleRowTwoCellsWorkflowConfiguration singleRowTwoCellsConfig();

    RingWorkflowConfiguration ringConfig();

    TransientRowsWorkflowConfiguration transientRowsConfig();

    SingleBusyCellWorkflowConfiguration singleBusyCellConfig();

    BankBalanceWorkflowConfiguration bankBalanceConfig();

    RandomWorkflowConfiguration randomConfig();

    SingleBusyCellReadsNoTouchWorkflowConfiguration singleBusyCellReadsNoTouchConfig();

    boolean exitAfterRunning();
}
