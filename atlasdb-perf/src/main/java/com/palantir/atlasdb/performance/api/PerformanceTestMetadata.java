/**
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
 */
package com.palantir.atlasdb.performance.api;

import static java.lang.annotation.ElementType.TYPE;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * This annotation is used to identify classes that can be run as a performance test.
 *
 * A performance test is universally identified by its {@code name} and {@code version}. Note that if a class has this
 * annotation it should implement the {@link PerformanceTest} interface.
 *
 * @author mwakerman
 *
 */
@Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@Target({TYPE})
public @interface PerformanceTestMetadata {

    /**
     * The name of the performance test. This is determines how the CLI runs the performance test. For example, if the
     * {@code name} value is {@code test-single-puts}, then the test is invoked by running:
     *
     *          ./atlasdb-perf --test test-single-puts --backend $BACKEND
     *
     * @return the name of the performance test.
     */
    String name();

    /**
     * The version of the performance test. For a given test name, results are only comparable to results from the same
     * version number. The intention of the test version number is to allow for small but necessary modifications to
     * performance tests without needing to delete and re-create tests and prevent name collisions.
     *
     * Version numbers should start at one and increment by one each time the test is updated.
     *
     * @return the integer version of the performance test.
     */
    int version();
}
