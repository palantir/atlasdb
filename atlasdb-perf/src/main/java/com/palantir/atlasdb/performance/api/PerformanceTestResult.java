/*
 * Copyright 2016 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.palantir.atlasdb.performance.api;

import org.immutables.value.Value;
import org.joda.time.DateTime;

/**
 * Interface for the results of running a {@code PerformanceTest}.
 *
 * @author mwakerman
 */
@Value.Immutable
public interface PerformanceTestResult {

    /**
     * Returns the {@code PerformanceTestMetadata#name} of the test corresponding to this result.
     *
     * @return the name of the performance test corresponding with this result.
     */
    String getTestName();

    /**
     * Returns the {@code PerformanceTestMetadata#version} of the test corresponding to this result.
     *
     * @return the version of the performance test corresponding with this result.
     */
    int getTestVersion();

    /**
     * Returns the "result" of the performance test.
     *
     * This is currently the runtime of the test in milliseconds but may be expanded in the future.
     *
     * @return the runtime of the performance test in milliseconds.
     */
    double getResult();

    /**
     * Returns the time the test was run.
     *
     * @return the time in UTC that the test was run.
     */
    DateTime getTestTime();

}
