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

package com.palantir.atlasdb.timelock.clock;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class ClockSkewEventsTest {
    @Test
    public void fastAndCurrentRequestConsideredRepresentative() {
        // 1 nanosecond
        assertThat(ClockSkewEvents.requestHasLikelyRepresentativeSkew(1L, 1L)).isTrue();
    }

    @Test
    public void slowRequestNotConsideredRepresentative() {
        assertThat(ClockSkewEvents.requestHasLikelyRepresentativeSkew(
                1L, 10 * ClockSkewEvents.REPRESENTATIVE_REQUEST_DURATION.toNanos())).isFalse();
    }

    @Test
    public void outOfDateRequestNotConsideredRepresentative() {
        assertThat(ClockSkewEvents.requestHasLikelyRepresentativeSkew(
                10 * ClockSkewEvents.REPRESENTATIVE_INTERVAL_SINCE_PREVIOUS_REQUEST.toNanos(), 1L)).isFalse();
    }

    @Test
    public void slowAndOutOfDateRequestNotConsideredRepresentative() {
        assertThat(ClockSkewEvents.requestHasLikelyRepresentativeSkew(
                10 * ClockSkewEvents.REPRESENTATIVE_INTERVAL_SINCE_PREVIOUS_REQUEST.toNanos(),
                10 * ClockSkewEvents.REPRESENTATIVE_REQUEST_DURATION.toNanos())).isFalse();
    }
}
