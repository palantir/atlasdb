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

package com.palantir.util;

import com.fasterxml.jackson.annotation.JsonIgnoreType;
import java.util.List;
import java.util.Map;
import org.immutables.value.Value;

/**
 * This class is merely used to wrap the output of {@link IndexEncodingUtils#encode} and should not be embedded in
 * any other object directly or serialized as-is.
 */
@Value.Immutable
@JsonIgnoreType
public interface IndexEncodingResult<K, V> {
    @Value.Parameter
    List<K> keyList();

    @Value.Parameter
    Map<Integer, V> indexToValue();

    @Value.Parameter
    long keyListCrc32Checksum();

    static <K, V> IndexEncodingResult<K, V> of(
            List<K> keyList, Map<Integer, V> indexToValue, long keyListCrc32Checksum) {
        return ImmutableIndexEncodingResult.of(keyList, indexToValue, keyListCrc32Checksum);
    }
}
