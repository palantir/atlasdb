/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock.lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;

import io.dropwizard.jackson.Jackson;
import io.dropwizard.testing.ResourceHelpers;

public class BlockingTimeoutsIntegrationTest {
    private static final ObjectMapper OBJECT_MAPPER = Jackson.newObjectMapper(new YAMLFactory());

    private static final int MILLIS_PER_SECOND = 1000;
    private static final int SECONDS_IN_ONE_DAY = 86400;

    private static final double ERROR_MARGIN = 0.03;

    @Test
    public void returnsDefaultBlockingTimeoutWithConnectorsWithoutIdleTimeouts() throws IOException {
        assertThat(BlockingTimeouts.getBlockingTimeout(OBJECT_MAPPER, getConfigurationFromResource(
                "lock/default-timeout.yml")))
                .isEqualTo(BlockingTimeouts.scaleForErrorMargin(BlockingTimeouts.DEFAULT_IDLE_TIMEOUT, ERROR_MARGIN));
    }

    @Test
    public void returnsBlockingTimeoutWithConnectorWithIdleTimeouts() throws IOException {
        assertThat(BlockingTimeouts.getBlockingTimeout(OBJECT_MAPPER, getConfigurationFromResource(
                "lock/one-specified-timeout.yml")))
                .isEqualTo(BlockingTimeouts.scaleForErrorMargin(SECONDS_IN_ONE_DAY * MILLIS_PER_SECOND, ERROR_MARGIN));
    }

    @Test
    public void returnsShortestBlockingTimeoutWithMultipleConnectorsWithIdleTimeouts() throws IOException {
        assertThat(BlockingTimeouts.getBlockingTimeout(OBJECT_MAPPER, getConfigurationFromResource(
                "lock/two-specified-timeouts.yml")))
                .isEqualTo(BlockingTimeouts.scaleForErrorMargin(60 * MILLIS_PER_SECOND, ERROR_MARGIN));
    }

    @Test
    public void returnsDefaultBlockingTimeoutIfConnectorWithoutSpecifiedIdleTimeoutIsMinimal() throws IOException {
        assertThat(BlockingTimeouts.getBlockingTimeout(OBJECT_MAPPER, getConfigurationFromResource(
                "lock/one-default-one-higher-explicit-timeout.yml")))
                .isEqualTo(BlockingTimeouts.scaleForErrorMargin(BlockingTimeouts.DEFAULT_IDLE_TIMEOUT, ERROR_MARGIN));
    }

    @Test
    public void cannotGetBlockingTimeoutIfTimeLimitNotEnabled() throws IOException {
        assertThatThrownBy(() -> BlockingTimeouts.getBlockingTimeout(OBJECT_MAPPER, getConfigurationFromResource(
                "lock/time-limiting-disabled.yml")))
                .isInstanceOf(IllegalStateException.class);
    }

    private TimeLockServerConfiguration getConfigurationFromResource(String fileName) throws IOException {
        try (InputStream stream = new FileInputStream(ResourceHelpers.resourceFilePath(fileName))) {
            return OBJECT_MAPPER.readValue(stream, TimeLockServerConfiguration.class);
        }
    }
}
