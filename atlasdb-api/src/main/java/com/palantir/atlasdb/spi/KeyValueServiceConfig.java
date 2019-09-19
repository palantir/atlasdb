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
package com.palantir.atlasdb.spi;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import java.util.Optional;

/**
 * Marker interface for various AtlasDb KeyValueService config objects.
 */
@JsonTypeInfo(use = Id.NAME, include = As.PROPERTY, property = "type", visible = false)
public interface KeyValueServiceConfig {
    String type();

    Optional<String> namespace();

    /**
     * The size of the thread pool used for concurrently running range requests.
     */
    int concurrentGetRangesThreadPoolSize();

    /**
     * The maximum number of threads from the pool of {@link #concurrentGetRangesThreadPoolSize()} to use
     * for a single getRanges request when the user does not explicitly provide a value.
     */
    default int defaultGetRangesConcurrency() {
        return Math.min(8, concurrentGetRangesThreadPoolSize() / 2);
    }

}
