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

import java.util.StringJoiner;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Static utilities class for handling {@code PerformanceTest}s and associated classes.
 *
 * @author mwakerman
 */
public class PerformanceTestUtils {

    public static String toCsvLine(PerformanceTestResult result) {
        return new StringJoiner(",","","\n")
                .add(result.getTestTime().withZone(DateTimeZone.UTC).toString())
                .add(result.getTestName())
                .add(result.getTestVersion()+"")
                .add(result.getResult()+"").toString();
    }

    public static PerformanceTestResult fromCsvLine(String line) {
        final String[] components = line.split(",");
        return ImmutablePerformanceTestResult.builder()
                .testTime(DateTime.parse(components[0]))
                .testName(components[1])
                .testVersion(Integer.parseInt(components[2]))
                .result(Double.parseDouble(components[3]))
                .build();
    }

}
