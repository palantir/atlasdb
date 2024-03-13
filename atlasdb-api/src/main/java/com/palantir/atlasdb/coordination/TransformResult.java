/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.coordination;

import com.palantir.common.annotations.ImmutablesStyles.PackageVisibleImmutablesStyle;
import org.immutables.value.Value;

/**
 * Indicates the result of a coordination service transform call.
 *
 * Regardless of whether the transformation was successful and managed to install the new value,
 * as requested by the user, the result will always contain the current value.
 *
 * Users should retry the operation accordingly if they care about their particular operation succeeding.
 *
 * @param <T> The type of the value returned by the transform.
 */
@Value.Immutable
@PackageVisibleImmutablesStyle
public interface TransformResult<T> {

    @Value.Parameter
    boolean successful();

    @Value.Parameter
    T value();

    static <T> TransformResult<T> of(boolean successful, T value) {
        return ImmutableTransformResult.of(successful, value);
    }
}
