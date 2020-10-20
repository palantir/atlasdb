/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.adjudicate;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Arrays;
import org.junit.Test;

public class HealthStateTest {

    @Test
    public void worstStatusOfAllOptions() {
        assertThat(getWorst(HealthStatus.HEALTHY, HealthStatus.UNKNOWN)).isEqualTo(HealthStatus.UNKNOWN);
        assertThat(getWorst(HealthStatus.HEALTHY, HealthStatus.UNHEALTHY)).isEqualTo(HealthStatus.UNHEALTHY);
        assertThat(getWorst(HealthStatus.HEALTHY, HealthStatus.HEALTHY)).isEqualTo(HealthStatus.HEALTHY);
        assertThat(getWorst(HealthStatus.UNKNOWN, HealthStatus.UNHEALTHY)).isEqualTo(HealthStatus.UNHEALTHY);
        assertThat(getWorst(HealthStatus.UNKNOWN, HealthStatus.UNHEALTHY, HealthStatus.HEALTHY))
                .isEqualTo(HealthStatus.UNHEALTHY);
    }

    private HealthStatus getWorst(HealthStatus... healthStatuses) {
        return Arrays.stream(healthStatuses)
                .max(HealthStatus.getHealthStatusComparator())
                .orElseThrow(() -> new SafeIllegalStateException("Attempted to get the worst of zero health statuses"));
    }
}
