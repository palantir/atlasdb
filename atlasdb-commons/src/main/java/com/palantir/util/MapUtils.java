/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class MapUtils {

    private MapUtils() {}

    public static <K, V> Map<K, V> combineMaps(Map<K, V> primary, Map<K, V> secondary) {
        return Stream.concat(primary.entrySet().stream(), secondary.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (valueOne, valueTwo) -> valueOne));
    }
}
