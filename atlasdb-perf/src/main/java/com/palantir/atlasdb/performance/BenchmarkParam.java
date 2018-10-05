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
package com.palantir.atlasdb.performance;

/**
 * Enum for keeping track of JMH parameters {@see @Param}. Edit this enum with caution.
 * The keys must match the name of their associated instance variable.
 */
public enum BenchmarkParam {
    URI("uri");

    private final String key;

    BenchmarkParam(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
