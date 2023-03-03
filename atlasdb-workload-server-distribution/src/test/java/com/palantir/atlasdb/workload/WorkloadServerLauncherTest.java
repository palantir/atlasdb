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

package com.palantir.atlasdb.workload;

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.Timer;
import com.palantir.atlasdb.workload.config.WorkloadServerConfiguration;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import java.util.Map;
import org.junit.ClassRule;
import org.junit.Test;

public class WorkloadServerLauncherTest {
    @ClassRule
    public static final DropwizardAppRule<WorkloadServerConfiguration> RULE = new DropwizardAppRule<>(
            WorkloadServerLauncher.class, ResourceHelpers.resourceFilePath("workload-server.yml"));

    @Test
    public void runsWorkflow() {
        TaggedMetricRegistry metricRegistry =
                ((WorkloadServerLauncher) RULE.getApplication()).getTaggedMetricRegistry();
        MetricName metricName = MetricName.builder()
                .safeName("com.palantir.atlasdb.keyvalue.api.KeyValueService.get")
                .safeTags(Map.of("libraryOrigin", "atlasdb"))
                .build();
        Timer kvsGetMetric = (Timer) metricRegistry.getMetrics().get(metricName);
        assertThat(kvsGetMetric.getCount())
                .as("Count of KeyValueService get calls should be greater than zero,"
                        + "as a workflow should have been executed")
                .isGreaterThan(0);
    }
}
