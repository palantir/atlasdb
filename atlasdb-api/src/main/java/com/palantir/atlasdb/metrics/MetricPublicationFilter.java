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

package com.palantir.atlasdb.metrics;

/**
 * Determines whether a metric should be published.
 */
public interface MetricPublicationFilter {
    MetricPublicationFilter NEVER_PUBLISH = () -> false;

    boolean shouldPublish();

    /**
     * @return A label which is used to deduplicate filters.
     *   Need only be unique across filters on a given metric.
     */
    default String getLabel() {
        return this.getClass().getName();
    }
}
