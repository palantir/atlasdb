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

package com.palantir.timelock.corruption.detection;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.palantir.paxos.NamespaceAndUseCase;
import org.immutables.value.Value;

@Value.Immutable
public interface CorruptionHealthReport {
    @Value.Parameter
    SetMultimap<CorruptionCheckViolation, NamespaceAndUseCase> violatingStatusesToNamespaceAndUseCase();

    static ImmutableCorruptionHealthReport.Builder builder() {
        return ImmutableCorruptionHealthReport.builder();
    }

    static CorruptionHealthReport defaultHealthyReport() {
        return CorruptionHealthReport.builder()
                .violatingStatusesToNamespaceAndUseCase(ImmutableSetMultimap.of())
                .build();
    }

    default boolean shouldRejectRequests() {
        return violatingStatusesToNamespaceAndUseCase().keySet().stream()
                .anyMatch(CorruptionCheckViolation::shouldRejectRequests);
    }
}
