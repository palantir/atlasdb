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

import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.stream.Stream;
import org.junit.Test;

public class CheckAndSetCompatibilityTest {
    private static final CheckAndSetCompatibility SUPPORTS_DETAIL_NOT_CONSISTENT_ON_FAILURE =
            CheckAndSetCompatibility.supportedBuilder()
                    .supportsDetailOnFailure(true)
                    .consistentOnFailure(false)
                    .build();
    private static final CheckAndSetCompatibility NO_DETAIL_CONSISTENT_ON_FAILURE =
            CheckAndSetCompatibility.supportedBuilder()
                    .supportsDetailOnFailure(false)
                    .consistentOnFailure(true)
                    .build();
    private static final CheckAndSetCompatibility SUPPORTS_DETAIL_CONSISTENT_ON_FAILURE =
            CheckAndSetCompatibility.supportedBuilder()
                    .supportsDetailOnFailure(true)
                    .consistentOnFailure(true)
                    .build();

    @Test
    public void checkingDetailSupportedOnUnsupportedThrows() {
        assertThatThrownBy(() -> CheckAndSetCompatibility.unsupported().supportsDetailOnFailure())
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("Should not check a KVS that does not support CAS operations for detail");
    }

    @Test
    public void checkingConsistencyOnUnsupportedThrows() {
        assertThatThrownBy(() -> CheckAndSetCompatibility.unsupported().consistentOnFailure())
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("Should not check a KVS that does not support CAS operations for consistency");
    }

    @Test
    public void unsupportedDoesNotSupportCheckAndSetOperations() {
        assertThat(CheckAndSetCompatibility.unsupported().supportsCheckAndSetOperations()).isFalse();
    }

    @Test
    public void supportedBuilderSupportsCheckAndSetOperations() {
        assertThat(CheckAndSetCompatibility.supportedBuilder()
                .supportsDetailOnFailure(false)
                .consistentOnFailure(false)
                .build()
                .supportsCheckAndSetOperations()).isTrue();
    }

    @Test
    public void intersectReturnsLeastRestrictiveWhenNoCompatibilitiesProvided() {
        CheckAndSetCompatibility intersection = intersectCompatibility();
        assertThat(intersection.supportsCheckAndSetOperations()).isTrue();
        assertThat(intersection.supportsDetailOnFailure()).isTrue();
        assertThat(intersection.consistentOnFailure()).isTrue();
    }

    @Test
    public void intersectAppliesToBothProperties() {
        CheckAndSetCompatibility intersection = intersectCompatibility(
                SUPPORTS_DETAIL_NOT_CONSISTENT_ON_FAILURE,
                NO_DETAIL_CONSISTENT_ON_FAILURE,
                SUPPORTS_DETAIL_CONSISTENT_ON_FAILURE);
        assertThat(intersection.supportsCheckAndSetOperations()).isTrue();
        assertThat(intersection.supportsDetailOnFailure()).isFalse();
        assertThat(intersection.consistentOnFailure()).isFalse();
    }

    @Test
    public void intersectReturnsDetailSupportedWhenAllSupport() {
        CheckAndSetCompatibility intersection = intersectCompatibility(
                SUPPORTS_DETAIL_NOT_CONSISTENT_ON_FAILURE,
                SUPPORTS_DETAIL_CONSISTENT_ON_FAILURE);
        assertThat(intersection.supportsCheckAndSetOperations()).isTrue();
        assertThat(intersection.supportsDetailOnFailure()).isTrue();
        assertThat(intersection.consistentOnFailure()).isFalse();
    }

    @Test
    public void intersectReturnsConsistentWhenAllConsistent() {
        CheckAndSetCompatibility intersection = intersectCompatibility(
                SUPPORTS_DETAIL_CONSISTENT_ON_FAILURE,
                NO_DETAIL_CONSISTENT_ON_FAILURE);
        assertThat(intersection.supportsCheckAndSetOperations()).isTrue();
        assertThat(intersection.supportsDetailOnFailure()).isFalse();
        assertThat(intersection.consistentOnFailure()).isTrue();
    }

    @Test
    public void intersectDoesNotRestrictUnnecessarily() {
        CheckAndSetCompatibility intersection = intersectCompatibility(
                SUPPORTS_DETAIL_CONSISTENT_ON_FAILURE,
                SUPPORTS_DETAIL_CONSISTENT_ON_FAILURE);
        assertThat(intersection.supportsCheckAndSetOperations()).isTrue();
        assertThat(intersection.supportsDetailOnFailure()).isTrue();
        assertThat(intersection.consistentOnFailure()).isTrue();
    }

    @Test
    public void intersectWithUnsupportedIsUnsupported() {
        CheckAndSetCompatibility intersection = intersectCompatibility(
                SUPPORTS_DETAIL_CONSISTENT_ON_FAILURE,
                CheckAndSetCompatibility.unsupported());
        assertThat(intersection.supportsCheckAndSetOperations()).isFalse();
    }

    private CheckAndSetCompatibility intersectCompatibility(CheckAndSetCompatibility... compatibilities) {
        return CheckAndSetCompatibility.intersect(Stream.of(compatibilities));
    }
}
