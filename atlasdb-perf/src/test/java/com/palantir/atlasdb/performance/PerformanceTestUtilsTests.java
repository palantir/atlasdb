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

package com.palantir.atlasdb.performance;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

import com.palantir.atlasdb.performance.api.ImmutablePerformanceTestResult;
import com.palantir.atlasdb.performance.api.PerformanceTestResult;
import com.palantir.atlasdb.performance.api.PerformanceTestUtils;

/**
 * Unit tests for the {@link PerformanceTestUtils} class.
 *
 * @author mwakerman
 */
public class PerformanceTestUtilsTests {

    @Test
    public void testPerformanceTestResultToFromCsv() {
        final PerformanceTestResult ptr = ImmutablePerformanceTestResult.builder()
                .result(9999)
                .testName("test-put-thing")
                .testTime(new DateTime().withZone(DateTimeZone.UTC))
                .testVersion(2)
                .build();

        final String ptrString = PerformanceTestUtils.toCsvLine(ptr);

        assertTrue(PerformanceTestUtils.fromCsvLine(ptrString).equals(ptr));
    }
}
