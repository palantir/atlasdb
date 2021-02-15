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
package com.palantir.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class OptionalResolver {
    private OptionalResolver() {
        // Utility class
    }

    /**
     * Returns a single value corresponding to the value that is present in one or more of the Optionals provided.
     * This method throws if no Optionals provided contain values, or if the Optionals provided contain multiple
     * values that are not equal.
     * Null Optionals are considered not-present.
     */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType") // Used to process existing configuration files.
    public static <T> T resolve(Optional<T> optional1, Optional<T> optional2) {
        Set<T> values = Stream.of(optional1, optional2)
                .filter(Objects::nonNull)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toSet());

        com.palantir.logsafe.Preconditions.checkArgument(
                values.size() >= 1, "All Optionals provided were empty, couldn't determine a value.");
        Preconditions.checkArgument(
                values.size() <= 1, "Contradictory values %s found, expected a single common value", values);
        return Iterables.getOnlyElement(values);
    }
}
