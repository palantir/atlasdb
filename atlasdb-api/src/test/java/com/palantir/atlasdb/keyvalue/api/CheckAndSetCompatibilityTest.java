/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.Test;

public class CheckAndSetCompatibilityTest {
    @Test
    public void intersectReturnsLeastRestrictiveWhenNoCompatibilitiesProvided() {
        CheckAndSetCompatibility minCompatibility = intersectCompatibility();
        assertThat(minCompatibility.supportsDetailOnFailure()).isFalse();
        assertThat(minCompatibility.consistentOnFailure()).isFalse();
    }

    @Test
    public void intersectAppliesToBothProperties() {
        CheckAndSetCompatibility minCompatibility = intersectCompatibility(
                CheckAndSetCompatibility.SUPPORTS_DETAIL_NOT_CONSISTENT_ON_FAILURE,
                CheckAndSetCompatibility.NO_DETAIL_CONSISTENT_ON_FAILURE,
                CheckAndSetCompatibility.SUPPORTS_DETAIL_CONSISTENT_ON_FAILURE);
        assertThat(minCompatibility.supportsDetailOnFailure()).isFalse();
        assertThat(minCompatibility.consistentOnFailure()).isFalse();
    }

    @Test
    public void intersectReturnsDetailSupportedWhenAllSupport() {
        CheckAndSetCompatibility minCompatibility = intersectCompatibility(
                CheckAndSetCompatibility.SUPPORTS_DETAIL_NOT_CONSISTENT_ON_FAILURE,
                CheckAndSetCompatibility.SUPPORTS_DETAIL_CONSISTENT_ON_FAILURE);
        assertThat(minCompatibility.supportsDetailOnFailure()).isTrue();
        assertThat(minCompatibility.consistentOnFailure()).isFalse();
    }

    @Test
    public void intersectReturnsConsistentWhenAllConsistent() {
        CheckAndSetCompatibility minCompatibility = intersectCompatibility(
                CheckAndSetCompatibility.SUPPORTS_DETAIL_CONSISTENT_ON_FAILURE,
                CheckAndSetCompatibility.NO_DETAIL_CONSISTENT_ON_FAILURE);
        assertThat(minCompatibility.supportsDetailOnFailure()).isFalse();
        assertThat(minCompatibility.consistentOnFailure()).isTrue();
    }

    @Test
    public void intersectDoesNotRestrictUnnecessarily() {
        CheckAndSetCompatibility minCompatibility = intersectCompatibility(
                CheckAndSetCompatibility.SUPPORTS_DETAIL_CONSISTENT_ON_FAILURE,
                CheckAndSetCompatibility.SUPPORTS_DETAIL_CONSISTENT_ON_FAILURE);
        assertThat(minCompatibility.supportsDetailOnFailure()).isTrue();
        assertThat(minCompatibility.consistentOnFailure()).isTrue();
    }

    private CheckAndSetCompatibility intersectCompatibility(CheckAndSetCompatibility... compatibilities) {
        return CheckAndSetCompatibility.intersect(Stream.of(compatibilities));
    }
}
