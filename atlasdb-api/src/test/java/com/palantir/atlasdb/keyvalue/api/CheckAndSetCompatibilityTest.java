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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.Stream;
import org.junit.Test;

public class CheckAndSetCompatibilityTest {
    private static final CheckAndSetCompatibility DETAIL_ONLY = CheckAndSetCompatibility.supportedBuilder()
            .supportsMultiCheckAndSetOperations(false)
            .supportsDetailOnFailure(true)
            .consistentOnFailure(false)
            .build();
    private static final CheckAndSetCompatibility CONSISTENT_ONLY = CheckAndSetCompatibility.supportedBuilder()
            .supportsMultiCheckAndSetOperations(false)
            .supportsDetailOnFailure(false)
            .consistentOnFailure(true)
            .build();
    private static final CheckAndSetCompatibility MCAS_ONLY = CheckAndSetCompatibility.supportedBuilder()
            .supportsMultiCheckAndSetOperations(true)
            .supportsDetailOnFailure(false)
            .consistentOnFailure(false)
            .build();
    private static final CheckAndSetCompatibility DETAIL_AND_CONSISTENT = CheckAndSetCompatibility.supportedBuilder()
            .supportsMultiCheckAndSetOperations(false)
            .supportsDetailOnFailure(true)
            .consistentOnFailure(true)
            .build();

    private static final CheckAndSetCompatibility MCAS_DETAIL_CONSISTENT = CheckAndSetCompatibility.supportedBuilder()
            .supportsMultiCheckAndSetOperations(true)
            .supportsDetailOnFailure(true)
            .consistentOnFailure(true)
            .build();

    @Test
    public void checkingDetailSupportedOnUnsupportedThrows() {
        assertThatThrownBy(() -> CheckAndSetCompatibility.unsupported().supportsDetailOnFailure())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Should not check a KVS that does not support CAS operations for detail");
    }

    @Test
    public void checkingConsistencyOnUnsupportedThrows() {
        assertThatThrownBy(() -> CheckAndSetCompatibility.unsupported().consistentOnFailure())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Should not check a KVS that does not support CAS operations for consistency");
    }

    @Test
    public void unsupportedDoesNotSupportCheckAndSetOperations() {
        assertThat(CheckAndSetCompatibility.unsupported().supportsCheckAndSetOperations())
                .isFalse();
    }

    @Test
    public void unsupportedDoesNotSupportMultiCheckAndSetOperations() {
        assertThat(CheckAndSetCompatibility.unsupported().supportsMultiCheckAndSetOperations())
                .isFalse();
    }

    @Test
    public void supportedBuilderSupportsCheckAndSetOperations() {
        assertThat(CheckAndSetCompatibility.supportedBuilder()
                        .supportsMultiCheckAndSetOperations(false)
                        .supportsDetailOnFailure(false)
                        .consistentOnFailure(false)
                        .build()
                        .supportsCheckAndSetOperations())
                .isTrue();
    }

    @Test
    public void intersectReturnsLeastRestrictiveWhenNoCompatibilitiesProvided() {
        CheckAndSetCompatibility intersection = intersectCompatibility();
        assertThat(intersection.supportsCheckAndSetOperations()).isTrue();
        assertThat(intersection.supportsMultiCheckAndSetOperations()).isTrue();
        assertThat(intersection.supportsDetailOnFailure()).isTrue();
        assertThat(intersection.consistentOnFailure()).isTrue();
    }

    @Test
    public void intersectAppliesToAllProperties() {
        CheckAndSetCompatibility intersection = intersectCompatibility(
                DETAIL_ONLY, CONSISTENT_ONLY, MCAS_ONLY, DETAIL_AND_CONSISTENT, MCAS_DETAIL_CONSISTENT);
        assertThat(intersection.supportsCheckAndSetOperations()).isTrue();
        assertThat(intersection.supportsMultiCheckAndSetOperations()).isFalse();
        assertThat(intersection.supportsDetailOnFailure()).isFalse();
        assertThat(intersection.consistentOnFailure()).isFalse();
    }

    @Test
    public void intersectReturnsDetailSupportedWhenAllSupport() {
        CheckAndSetCompatibility intersection = intersectCompatibility(DETAIL_ONLY, DETAIL_AND_CONSISTENT);
        assertThat(intersection.supportsCheckAndSetOperations()).isTrue();
        assertThat(intersection.supportsMultiCheckAndSetOperations()).isFalse();
        assertThat(intersection.supportsDetailOnFailure()).isTrue();
        assertThat(intersection.consistentOnFailure()).isFalse();
    }

    @Test
    public void intersectReturnsConsistentWhenAllConsistent() {
        CheckAndSetCompatibility intersection = intersectCompatibility(DETAIL_AND_CONSISTENT, CONSISTENT_ONLY);
        assertThat(intersection.supportsCheckAndSetOperations()).isTrue();
        assertThat(intersection.supportsMultiCheckAndSetOperations()).isFalse();
        assertThat(intersection.supportsDetailOnFailure()).isFalse();
        assertThat(intersection.consistentOnFailure()).isTrue();
    }

    @Test
    public void intersectReturnsMultiCheckAndSetOperationsSupportedWhenAllSupport() {
        CheckAndSetCompatibility intersection = intersectCompatibility(MCAS_ONLY, MCAS_DETAIL_CONSISTENT);
        assertThat(intersection.supportsCheckAndSetOperations()).isTrue();
        assertThat(intersection.supportsMultiCheckAndSetOperations()).isTrue();
        assertThat(intersection.supportsDetailOnFailure()).isFalse();
        assertThat(intersection.consistentOnFailure()).isFalse();
    }

    @Test
    public void intersectDoesNotRestrictUnnecessarily() {
        CheckAndSetCompatibility intersection = intersectCompatibility(MCAS_DETAIL_CONSISTENT, MCAS_DETAIL_CONSISTENT);
        assertThat(intersection.supportsCheckAndSetOperations()).isTrue();
        assertThat(intersection.supportsMultiCheckAndSetOperations()).isTrue();
        assertThat(intersection.supportsDetailOnFailure()).isTrue();
        assertThat(intersection.consistentOnFailure()).isTrue();
    }

    @Test
    public void intersectWithUnsupportedIsUnsupported() {
        CheckAndSetCompatibility intersection =
                intersectCompatibility(DETAIL_AND_CONSISTENT, CheckAndSetCompatibility.unsupported());
        assertThat(intersection.supportsCheckAndSetOperations()).isFalse();
        assertThat(intersection.supportsMultiCheckAndSetOperations()).isFalse();
    }

    private CheckAndSetCompatibility intersectCompatibility(CheckAndSetCompatibility... compatibilities) {
        return CheckAndSetCompatibility.intersect(Stream.of(compatibilities));
    }
}
