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

package com.palantir.atlasdb.transaction.service;

import java.util.function.Function;

import com.palantir.atlasdb.AtlasDbConstants;

/**
 * A {@link PreStartTimestampHandler} is a {@link Function} from timestamps to specific values, that specially handles
 * timestamps before {@link AtlasDbConstants#STARTING_TS} by returning a default fallback value.
 *
 * @param <T> return type of the function
 */
public class PreStartTimestampHandler<T> implements Function<Long, T> {
    private final T defaultValue;
    private final Function<Long, T> delegate;

    PreStartTimestampHandler(T defaultValue, Function<Long, T> delegate) {
        this.defaultValue = defaultValue;
        this.delegate = delegate;
    }

    @Override
    public T apply(Long timestamp) {
        return timestamp < AtlasDbConstants.STARTING_TS ? defaultValue : delegate.apply(timestamp);
    }
}
