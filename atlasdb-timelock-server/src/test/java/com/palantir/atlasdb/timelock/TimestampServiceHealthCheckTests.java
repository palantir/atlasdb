/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.palantir.timestamp.TimestampService;

public class TimestampServiceHealthCheckTests {
    private TimestampService timestampService = mock(TimestampService.class);

    private TimestampServiceHealthCheck timestampServiceHealthCheck;

    @Before
    public void before() {
        timestampServiceHealthCheck = new TimestampServiceHealthCheck(timestampService);
    }

    @Test
    public void healthyIfCanGetCurrentTime() throws Exception {
        when(timestampService.getFreshTimestamp()).thenReturn(12345L);
        assertThat(timestampServiceHealthCheck.check().isHealthy()).isTrue();
    }

    @Test
    public void unhealthyIfCanGetCurrentTime() throws Exception {
        when(timestampService.getFreshTimestamp()).thenThrow(new RuntimeException());
        assertThat(timestampServiceHealthCheck.check().isHealthy()).isFalse();
    }

}
