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
package com.palantir.util.sql;

import javax.management.MXBean;

@MXBean
public interface SqlCallStatsMBean {
    String getQueryName();

    String getRawSql();

    void clearStats();

    long getTotalTime();

    long getTotalCalls();

    long getTimePerCallInMillis();

    double getStandardDeviationInMillis();

    long getMaxCallTime();

    long getMinCallTime();

    double getPercentileMillis(double perc);

    double getPercentCallsFinishedInMillis(int millis);

    double getMedianTimeRequestInMillis();

    // The following are redundant with getPercentileMillis, but they should
    // make information easier to browse from jconsole.
    double get25thPercentileCallTimeMillis();

    double get75thPercentileCallTimeMillis();
}
