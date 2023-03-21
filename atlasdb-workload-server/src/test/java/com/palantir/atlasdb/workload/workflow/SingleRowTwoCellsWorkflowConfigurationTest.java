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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.RateLimiter;
import com.palantir.atlasdb.workload.store.IsolationLevel;
import com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers;
import com.palantir.conjure.java.serialization.ObjectMappers;
import org.junit.Test;

public class SingleRowTwoCellsWorkflowConfigurationTest {
    private static final TableConfiguration TABLE_CONFIGURATION = ImmutableTableConfiguration.builder()
            .tableName(WorkloadTestHelpers.TABLE_1)
            .isolationLevel(IsolationLevel.SERIALIZABLE)
            .build();
    private static final ObjectMapper OBJECT_MAPPER = ObjectMappers.newServerObjectMapper();

    static {
        OBJECT_MAPPER.registerSubtypes(SingleRowTwoCellsWorkflowConfiguration.class);
    }

    @Test
    public void rateLimiterCreatedWithConfiguredNumberOfPermits() {
        SingleRowTwoCellsWorkflowConfiguration configuration = createWithRateLimit(Double.MIN_NORMAL);
        RateLimiter rateLimiter = configuration.transactionRateLimiter();
        assertThat(rateLimiter.tryAcquire()).isTrue();
        assertThat(rateLimiter.tryAcquire())
                .as("the rate limiter granted two requests, but was not configured to accept more than one every"
                        + " ~2.2 * 10^308 seconds")
                .isFalse();
    }

    @Test
    public void deserializationIsInverseOfSerialization() throws JsonProcessingException {
        SingleRowTwoCellsWorkflowConfiguration configuration = createWithRateLimit(5);
        assertThat(OBJECT_MAPPER.readValue(
                        OBJECT_MAPPER.writeValueAsString(configuration), WorkflowConfiguration.class))
                .isEqualTo(configuration);
    }

    @Test
    public void rateLimitAccountedForInSerializedForm() throws JsonProcessingException {
        assertThat(OBJECT_MAPPER.writeValueAsString(createWithRateLimit(10)))
                .isNotEqualTo(OBJECT_MAPPER.writeValueAsString(createWithRateLimit(20)));
    }

    private static SingleRowTwoCellsWorkflowConfiguration createWithRateLimit(double rateLimit) {
        return ImmutableSingleRowTwoCellsWorkflowConfiguration.builder()
                .tableConfiguration(TABLE_CONFIGURATION)
                .rateLimit(rateLimit)
                .iterationCount(10)
                .build();
    }
}
