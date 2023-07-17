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

package com.palantir.util.io;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public final class AvailabilityRequirementTest {
    @Test
    public void anySatisfiesGreaterThanZero() {
        assertThat(AvailabilityRequirement.ANY.satisfies(1, 10)).isTrue();
    }

    @Test
    public void anySatisfiesWhenEquivalentToTotal() {
        assertThat(AvailabilityRequirement.ANY.satisfies(10, 10)).isTrue();
    }

    @Test
    public void anyDoesNotSatisfyWhenZero() {
        assertThat(AvailabilityRequirement.ANY.satisfies(0, 10)).isFalse();
    }

    @Test
    public void quorumSatisfiesWhenHasMajority() {
        assertThat(AvailabilityRequirement.QUORUM.satisfies(2, 3)).isTrue();
        assertThat(AvailabilityRequirement.QUORUM.satisfies(4, 6)).isTrue();
        assertThat(AvailabilityRequirement.QUORUM.satisfies(5, 9)).isTrue();
    }

    @Test
    public void quorumDoesNotSatisfyWhenItDoesNotHaveMajority() {
        assertThat(AvailabilityRequirement.QUORUM.satisfies(1, 3)).isFalse();
        assertThat(AvailabilityRequirement.QUORUM.satisfies(3, 6)).isFalse();
    }

    @Test
    public void quorumSatisfiesWhenHasTotal() {
        assertThat(AvailabilityRequirement.QUORUM.satisfies(10, 10)).isTrue();
    }
}
